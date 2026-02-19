import logging
import sys
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import rowcol_to_a1
import re
import os
from gspread.exceptions import APIError


# ---------------- НАСТРОЙКИ ------------------
LOG_FILE = "keyword_kz.log"

# НЕ ТРОГАЮ: дефолтный токен как был, но если в env задан TOKEN — используем его (чтобы Actions работал через secrets)
DEFAULT_TOKEN = "kPQGRMFx7JYdJ3mqQyqGF62CRtPGKTb7"
TOKEN = os.getenv("TOKEN", DEFAULT_TOKEN)

EXCEL_FILE = "keywords_full_data.xlsx"
CREDS_FILE = "level-landing-195008-a8940ac6b2ab.json"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1tvLCWC5WhBnQAFQpoJsVUXnjtDlzjRbJwzmsYgM8b8c/edit?gid=639673523#gid=639673523"

# Пауза между запросами к keyword.com (оставляю как было, но можно тюнить env)
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "2"))

# Антизависание HTTP
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))

# Отладочные дампы (по умолчанию ВЫКЛ, чтобы не раздувать диск)
DEBUG_DUMP = os.getenv("DEBUG_DUMP", "0") in ("1", "true", "True")

# Частота прогресса в логах (по умолчанию 50)
LOG_EVERY_N = int(os.getenv("LOG_EVERY_N", "50"))

# Паузы после операций с Google Sheets (по умолчанию 2 сек, как у тебя)
GS_SLEEP = float(os.getenv("GS_SLEEP", "2"))
SHEET_SLEEP = float(os.getenv("SHEET_SLEEP", "3"))

# ---------------- ЛОГИРОВАНИЕ ------------------
logging.basicConfig(
    filename=LOG_FILE,
    filemode="w",
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Логи в stdout (чтобы GitHub Actions показывал прогресс)
_console = logging.StreamHandler(sys.stdout)
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_console)


# ---------------- УТИЛИТЫ ------------------
def retry_on_quota(func, *args, max_attempts=5, initial_delay=10, **kwargs):
    """
    Пробует вызвать func(*args, **kwargs), при APIError 429 — ждёт и повторяет.
    """
    delay = initial_delay
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            text = str(e)
            if "429" in text or "quota" in text.lower():
                logger.warning(
                    "Quota exceeded (attempt %d/%d). Sleep %s sec and retry...",
                    attempt,
                    max_attempts,
                    delay,
                )
                time.sleep(delay)
                delay *= 2
                continue
            raise
    logger.error("Quota retry failed for %s after %d attempts", func.__name__, max_attempts)
    raise RuntimeError(f"Quota retry failed: {func.__name__}")


def find_first_empty_row_in_col_A(ws):
    col_a = ws.col_values(1)
    for idx, val in enumerate(col_a, start=1):
        if not str(val).strip():
            return idx
    return len(col_a) + 1


def authorize_gspread(creds_file):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)  # type: ignore
    return gspread.authorize(creds)  # type: ignore


def find_analysis_sheets(spreadsheet_url, creds_file):
    """Возвращает список Worksheet, в названии которых есть 'анализ'."""
    gc = authorize_gspread(creds_file)
    sh = gc.open_by_url(spreadsheet_url)
    return [ws for ws in sh.worksheets() if "анализ" in ws.title.lower()]


def get_dates():
    """
    ВАЖНО:
    - Для API берём "за вчера" (обычно метрики стабильны за завершённый день).
    - В таблицу пишем "сегодня" (дата запуска).
    """
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    twoweek = today - timedelta(days=14)

    single = yesterday.strftime("%Y-%m-%d")  # API date
    twoweek_s = twoweek.strftime("%Y-%m-%d")
    date_range = f"{twoweek_s}.{single}"
    target = yesterday.strftime("%b %d, %Y")  # API history date

    gs_date = today.strftime("%d.%m.%Y")  # дата запуска в гугл-таблицу

    return single, date_range, target, gs_date


# ---------------- KEYWORD.COM API ------------------
def fetch_keywords(project_name, single_date, headers):
    url = f"https://app.keyword.com/api/v2/groups/{project_name}/keywords/"
    params = {"per_page": 250, "page": 1, "date": single_date}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logger.exception("(%s) HTTP error keywords: %s", project_name, e)
        return []

    if resp.status_code != 200:
        logger.error("(%s) keywords status=%s body=%s", project_name, resp.status_code, resp.text[:500])
        return []

    return resp.json().get("data", [])


def fetch_competitors_history(project_name, keyword_id, date_range, headers):
    url = f"https://app.keyword.com/api/v2/metrics/{project_name}/competitors/{keyword_id}/history"
    params = {"dateRange": date_range}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logger.exception("(%s) HTTP error history keyword_id=%s: %s", project_name, keyword_id, e)
        return []

    if resp.status_code != 200:
        logger.error(
            "(%s) history status=%s keyword_id=%s body=%s",
            project_name,
            resp.status_code,
            keyword_id,
            resp.text[:500],
        )
        return []

    # дамп полного ответа — только при DEBUG_DUMP=1
    if DEBUG_DUMP:
        dump_dir = "debug_history"
        os.makedirs(dump_dir, exist_ok=True)
        fname = os.path.join(dump_dir, f"{project_name}_{keyword_id}_{date_range}.json")
        try:
            with open(fname, "w", encoding="utf-8") as f:
                f.write(resp.text)
        except Exception as e:
            logger.warning("Cannot save history dump %s: %s", fname, e)

    try:
        data = resp.json().get("data", [])
    except Exception as e:
        logger.exception("JSON parse error history %s/%s: %s", project_name, keyword_id, e)
        return []

    return data


# ---------------- EXCEL BUILD ------------------
def build_sheet_data(project_name, keywords_data, date_range, target_date, headers):
    """
    first  — строки для листа Keywords,
    second — строки для листа Competitors.

    Если в history нет блока по target_date — берём самый свежий блок.
    """
    first, second = [], []

    total = len(keywords_data)

    for idx, item in enumerate(keywords_data, start=1):
        kid = item["id"]
        attr = item.get("attributes", {})
        kw = attr.get("kw")
        ru = attr.get("rankingurl")
        ua = attr.get("updated_at")

        first.append({"ID": kid, "Keyword": kw, "Ranking URL": ru, "Updated At": ua})

        history = fetch_competitors_history(project_name, kid, date_range, headers)

        chosen = next((blk for blk in history if blk.get("date") == target_date), None)

        if chosen is None and history:
            def parse_blk_date(b):
                try:
                    return datetime.strptime(b.get("date", ""), "%b %d, %Y")
                except Exception:
                    return datetime.min

            chosen = max(history, key=parse_blk_date)

        if chosen:
            ctr = 1
            for res in chosen.get("results", []):
                second.append({"ID": kid, "Keyword": kw, "URL": res.get("url"), "Row Number": ctr})
                ctr += 1

        if idx % LOG_EVERY_N == 0 or idx == total:
            logger.info("(%s) processed keywords: %d/%d", project_name, idx, total)

        time.sleep(RATE_LIMIT_DELAY)

    return first, second


def save_to_excel(first_sheet, second_sheet, filename):
    with pd.ExcelWriter(filename) as w:
        pd.DataFrame(first_sheet).to_excel(w, sheet_name="Keywords", index=False)
        pd.DataFrame(second_sheet).to_excel(w, sheet_name="Competitors", index=False)
    logger.info("Excel exported: %s", filename)


# ---------------- GOOGLE SHEETS ------------------
def normalize_url(u: str) -> str:
    u = u.strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    return u.rstrip("/")


def build_gs_map(ws):
    """
    keyword -> row_number из столбца B (только keyword, без URL).
    """
    col_b = ws.col_values(2)  # B
    gs_map = {}
    for i in range(1, len(col_b)):  # пропускаем заголовок
        kw = col_b[i].strip().lower()
        if kw:
            gs_map[kw] = i + 1
    return gs_map


def build_competitors_map(comp_df):
    """
    (keyword, normalized_url) -> [row_numbers]
    """
    cmap = {}
    for _, r in comp_df.iterrows():
        kw = str(r["Keyword"]).strip().lower()
        u = normalize_url(str(r["URL"]))
        rn = r["Row Number"]
        cmap.setdefault((kw, u), []).append(rn)
    return cmap


def build_competitors_keyword_map(comp_df, domain):
    """
    Keyword -> список URL конкурентов (без текущего домена и запрещённых подстрок)
    """
    kw_map = {}
    forbidden_substrings = ["2gis", "m.olx", "kaspi", "olx"]
    for _, r in comp_df.iterrows():
        kw = str(r["Keyword"]).strip().lower()
        url = str(r["URL"]).strip()
        url_lc = url.lower()

        if domain in url:
            continue
        if any(sub in url_lc for sub in forbidden_substrings):
            continue

        kw_map.setdefault(kw, []).append(url)
    return kw_map


def update_google_sheet(ws, kw_df, comp_map, gs_map, comp_kw_map, gs_date):
    CITY_COL = 4
    COMP_COLS = [13, 20, 27]  # M, T, AA
    FORMULA_COLS = [11, 19, 26, 33, 39, 45]  # K, S, Z, AG, AM, AS

    matched = []
    new = []

    for _, r in kw_df.iterrows():
        kw = str(r["Keyword"]).strip()
        ru0 = str(r["Ranking URL"]).strip()
        kw_lc = kw.lower()

        gs_row = gs_map.get(kw_lc)
        key = (kw_lc, normalize_url(ru0))
        rns = comp_map.get(key, [])
        urls = comp_kw_map.get(kw_lc, [])[:3]

        if gs_row is not None and rns:
            matched.append({"kw": kw, "ru0": ru0, "gs_row": gs_row, "rns": rns, "urls": urls})
        else:
            new.append({"kw": kw, "ru0": ru0, "rns": rns, "urls": urls})

    matched.sort(key=lambda x: x["gs_row"], reverse=True)

    # --- matched: вставляем строки под найденной строкой ---
    for item in matched:
        kw = item["kw"]
        ru0 = item["ru0"]
        gs_row = item["gs_row"]
        rns = item["rns"]
        urls = item["urls"]

        prev_city = ws.cell(gs_row, CITY_COL).value or ""
        new_rows = [[gs_date, kw, ru0, prev_city, rn] for rn in rns]
        insert_at = gs_row + 1
        num_rows = len(new_rows)

        retry_on_quota(ws.insert_rows, new_rows, row=insert_at)
        time.sleep(GS_SLEEP)

        try:
            sheet_id = ws._properties["sheetId"]
            copy_requests = []

            for offset in range(len(new_rows)):
                src_row = insert_at - 1
                dst_row = insert_at + offset

                for col in FORMULA_COLS:
                    copy_requests.append(
                        {
                            "copyPaste": {
                                "source": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": src_row - 1,
                                    "endRowIndex": src_row,
                                    "startColumnIndex": col - 1,
                                    "endColumnIndex": col,
                                },
                                "destination": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": dst_row - 1,
                                    "endRowIndex": dst_row,
                                    "startColumnIndex": col - 1,
                                    "endColumnIndex": col,
                                },
                                "pasteType": "PASTE_FORMULA",
                                "pasteOrientation": "NORMAL",
                            }
                        }
                    )

            if copy_requests:
                retry_on_quota(ws.spreadsheet.batch_update, {"requests": copy_requests})

            # Пишем конкурентные URL в первую вставленную строку (как у тебя было фактически)
            batch = []
            for i, url in enumerate(urls):
                cell = rowcol_to_a1(insert_at, COMP_COLS[i])
                batch.append({"range": f"'{ws.title}'!{cell}", "values": [[url]]})

            if batch:
                body = {"valueInputOption": "USER_ENTERED", "data": batch}
                retry_on_quota(ws.spreadsheet.values_batch_update, body)

        except RuntimeError as e:
            if "Quota retry failed" in str(e):
                ws.delete_rows(insert_at, insert_at + num_rows - 1)
                logger.warning("Rolled back %d inserted rows due to quota error", num_rows)
                continue
            raise

    # --- new: пишем в первую пустую строку по A ---
    batch_new = []
    if new:
        row_ptr = find_first_empty_row_in_col_A(ws)

        for item in new:
            kw = item["kw"]
            ru0 = item["ru0"]
            rns = item["rns"]
            urls = item["urls"]

            first_rn = rns[0] if rns else ""

            batch_new.append(
                {
                    "range": f"'{ws.title}'!A{row_ptr}:E{row_ptr}",
                    "values": [[gs_date, kw, ru0, "", first_rn]],
                }
            )

            for i, url in enumerate(urls):
                cell = rowcol_to_a1(row_ptr, COMP_COLS[i])
                batch_new.append({"range": f"'{ws.title}'!{cell}", "values": [[url]]})

            gs_map[kw.lower()] = row_ptr
            row_ptr += 1

    if batch_new:
        body = {"valueInputOption": "USER_ENTERED", "data": batch_new}
        retry_on_quota(ws.spreadsheet.values_batch_update, body)


# ---------------- MAIN PIPELINE ------------------
def process_sheet(ws, single_date, date_range, target_date, gs_date, headers):
    project_name = ws.acell("AT1").value
    if not project_name or not project_name.strip():
        logger.warning("Skip %s — AT1 is empty", ws.title)
        return

    project_name = project_name.strip()
    logger.info("=== Start sheet '%s' (project=%s) ===", ws.title, project_name)

    kws = fetch_keywords(project_name, single_date, headers)
    if not kws:
        logger.warning("No keywords for %s, skip", project_name)
        return

    fst, snd = build_sheet_data(project_name, kws, date_range, target_date, headers)

    save_to_excel(fst, snd, EXCEL_FILE)

    # доп. дампы Excel только при DEBUG_DUMP=1 (чтобы не раздувать диск)
    if DEBUG_DUMP:
        safe_title = ws.title.replace(" ", "_").replace("/", "_")
        copy_name = f"keywords_full_data_{safe_title}.xlsx"
        save_to_excel(fst, snd, copy_name)

    kw_df = pd.read_excel(EXCEL_FILE, sheet_name="Keywords")
    comp_df = pd.read_excel(EXCEL_FILE, sheet_name="Competitors")

    gs_map = build_gs_map(ws)
    comp_map = build_competitors_map(comp_df)
    comp_kw_map = build_competitors_keyword_map(comp_df, project_name)

    update_google_sheet(ws, kw_df, comp_map, gs_map, comp_kw_map, gs_date)

    logger.info("=== Done sheet '%s' ===", ws.title)


def main():
    single_date, date_range, target_date, gs_date = get_dates()
    headers = {"Authorization": f"Bearer {TOKEN}"}

    analysis_sheets = find_analysis_sheets(SPREADSHEET_URL, CREDS_FILE)

    logger.info("Run date (gs_date)=%s | API date=%s | range=%s", gs_date, single_date, date_range)
    logger.info("Sheets to process: %d", len(analysis_sheets))

    for ws in analysis_sheets:
        try:
            process_sheet(ws, single_date, date_range, target_date, gs_date, headers)
            time.sleep(SHEET_SLEEP)
        except Exception as e:
            logger.error("Sheet error %s: %s", ws.title, e, exc_info=True)

    logger.info("All done.")


if __name__ == "__main__":
    main()
