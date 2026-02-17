import logging
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import rowcol_to_a1
import re
import os
import random
from gspread.exceptions import APIError

# ---------------- НАСТРОЙКИ ------------------
LOG_FILE        = "keyword_kz.log"
TOKEN           = "kPQGRMFx7JYdJ3mqQyqGF62CRtPGKTb7"
EXCEL_FILE      = "keywords_full_data.xlsx"
CREDS_FILE      = "level-landing-195008-a8940ac6b2ab.json"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1tvLCWC5WhBnQAFQpoJsVUXnjtDlzjRbJwzmsYgM8b8c/edit?gid=639673523#gid=639673523"
RATE_LIMIT_DELAY= 2  # секунда между запросами (по умолчанию)

# --- ВАЖНО: берём из GitHub Actions env, если передали ---
TOKEN = os.getenv("TOKEN", TOKEN)
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", str(RATE_LIMIT_DELAY)))

logging.basicConfig(
    filename=LOG_FILE,
    filemode='w',
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def retry_on_quota(func, *args, max_attempts=5, initial_delay=10, **kwargs):
    """
    Пробует вызвать func(*args, **kwargs), при APIError 429 — ждёт и повторяет.
    (Это для Google Sheets API / gspread)
    """
    delay = initial_delay
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            text = str(e)
            if "429" in text or "quota" in text.lower():
                logger.warning("Quota exceeded (attempt %d/%d). Ждём %s сек и пробуем снова...",
                               attempt, max_attempts, delay)
                time.sleep(delay)
                delay *= 2
                continue
            raise
    logger.error("Не удалось выполнить %s после %d попыток — выходим", func.__name__, max_attempts)
    raise RuntimeError(f"Quota retry failed: {func.__name__}")


def keyword_get(url, headers, params=None, *, max_attempts=8, timeout=30,
                base_delay=0.7, max_delay=120):
    """
    Надёжный GET для keyword.com:
    - 429: уважает Retry-After (если есть) + backoff
    - 5xx/таймауты: backoff
    - 4xx (кроме 429): не ретраим
    """
    delay = base_delay

    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as e:
            sleep_s = min(delay, max_delay) * (1 + random.random() * 0.2)
            logger.warning("keyword.com network error (%s). attempt %d/%d, sleep %.1fs",
                           e, attempt, max_attempts, sleep_s)
            time.sleep(sleep_s)
            delay = min(delay * 2, max_delay)
            continue

        if resp.status_code == 200:
            return resp

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            sleep_s = int(ra) if (ra and ra.isdigit()) else min(delay, max_delay)
            sleep_s = sleep_s * (1 + random.random() * 0.2)
            logger.warning("keyword.com 429. attempt %d/%d, sleep %.1fs. url=%s",
                           attempt, max_attempts, sleep_s, url)
            time.sleep(sleep_s)
            delay = min(delay * 2, max_delay)
            continue

        if 500 <= resp.status_code <= 599:
            sleep_s = min(delay, max_delay) * (1 + random.random() * 0.2)
            logger.warning("keyword.com %s. attempt %d/%d, sleep %.1fs. body=%s",
                           resp.status_code, attempt, max_attempts, sleep_s, resp.text[:200])
            time.sleep(sleep_s)
            delay = min(delay * 2, max_delay)
            continue

        logger.error("keyword.com non-retriable status=%s url=%s body=%s",
                     resp.status_code, url, resp.text[:500])
        return resp

    raise RuntimeError(f"keyword.com failed after retries: {url}")


def find_first_empty_row_in_col_A(ws):
    col_a = ws.col_values(1)
    for idx, val in enumerate(col_a, start=1):
        if not str(val).strip():
            return idx
    return len(col_a) + 1


def authorize_gspread(creds_file):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)  # type: ignore
    return gspread.authorize(creds)  # type: ignore


def find_analysis_sheets(spreadsheet_url, creds_file):
    """Возвращает список Worksheet, в названии которых есть 'анализ'."""
    gc = authorize_gspread(creds_file)
    sh = gc.open_by_url(spreadsheet_url)
    return [ws for ws in sh.worksheets() if "анализ" in ws.title.lower()]


def get_dates():
    yesterday = datetime.today() - timedelta(days=1)
    twoweek = datetime.today() - timedelta(days=14)
    single = yesterday.strftime("%Y-%m-%d")
    twoweek = twoweek.strftime("%Y-%m-%d")
    date_range = f"{twoweek}.{single}"
    target = yesterday.strftime("%b %d, %Y")
    gs_date = yesterday.strftime("%d.%m.%Y")
    return single, date_range, target, gs_date


def fetch_keywords(project_name, single_date, headers):
    """
    ВАЖНО: тут добавлена пагинация — забираем ВСЕ страницы.
    """
    url = f"https://app.keyword.com/api/v2/groups/{project_name}/keywords/"
    all_data = []
    page = 1

    while True:
        params = {"per_page": 250, "page": page, "date": single_date}
        resp = keyword_get(url, headers=headers, params=params)

        if resp.status_code != 200:
            logger.error("(%s) Ошибка keywords page=%s: %s %s",
                         project_name, page, resp.status_code, resp.text[:500])
            break

        data = resp.json().get("data", [])
        all_data.extend(data)

        logger.info("(%s) keywords: page=%d got=%d total=%d",
                    project_name, page, len(data), len(all_data))

        if len(data) < 250:
            break

        page += 1
        time.sleep(0.2)

    return all_data


def fetch_competitors_history(project_name, keyword_id, date_range):
    url = f"https://app.keyword.com/api/v2/metrics/{project_name}/competitors/{keyword_id}/history"
    params = {"dateRange": date_range}

    resp = keyword_get(url, headers=HEADERS, params=params)

    if resp.status_code != 200:
        logger.error("(%s) history status=%s for %s: %s",
                     project_name, resp.status_code, keyword_id, resp.text[:500])
        return []

    try:
        data = resp.json().get("data", [])
    except Exception as e:
        logger.exception("Ошибка разбора JSON history для %s/%s: %s", project_name, keyword_id, e)
        return []

    return data


def build_sheet_data(project_name, keywords_data, date_range, target_date):
    first, second = [], []

    total = len(keywords_data)
    logger.info("(%s) build_sheet_data start: keywords=%d", project_name, total)

    for i, item in enumerate(keywords_data, start=1):
        kid = item["id"]
        attr = item.get("attributes", {})
        kw = attr.get("kw")
        ru = attr.get("rankingurl")
        ua = attr.get("updated_at")

        first.append({
            "ID": kid,
            "Keyword": kw,
            "Ranking URL": ru,
            "Updated At": ua
        })

        history = fetch_competitors_history(project_name, kid, date_range)

        chosen = next((blk for blk in history if blk.get("date") == target_date), None)

        if chosen is None and history:
            def parse_blk_date(b):
                try:
                    return datetime.strptime(b.get("date", ""), "%b %d, %Y")
                except Exception:
                    return datetime.min

            chosen = max(history, key=parse_blk_date)
            logger.info("(%s:%s) для даты %s нет данных, взяли самый свежий %s",
                        project_name, kid, target_date, chosen.get("date"))

        if chosen:
            ctr = 1
            for res in chosen.get("results", []):
                second.append({
                    "ID": kid,
                    "Keyword": kw,
                    "URL": res.get("url"),
                    "Row Number": ctr
                })
                ctr += 1

        if i == 1 or i % 50 == 0 or i == total:
            logger.info("(%s) progress keywords: %d/%d", project_name, i, total)

        time.sleep(RATE_LIMIT_DELAY)

    return first, second


def save_to_excel(first_sheet, second_sheet, filename):
    with pd.ExcelWriter(filename) as w:
        pd.DataFrame(first_sheet).to_excel(w, sheet_name="Keywords", index=False)
        pd.DataFrame(second_sheet).to_excel(w, sheet_name="Competitors", index=False)
    logger.info("Экспорт в Excel готов: %s", filename)


def normalize_url(u: str) -> str:
    u = u.strip().lower()
    u = re.sub(r'^https?://', '', u)
    u = re.sub(r'^www\.', '', u)
    return u.rstrip('/')


def read_google_sheet(worksheet):
    all_values = worksheet.get_all_values()
    if not all_values:
        return [], []
    return all_values[0], all_values[1:]


def build_gs_map(ws):
    col_b = ws.col_values(2)  # B
    gs_map = {}
    for i in range(1, len(col_b)):
        kw = col_b[i].strip().lower()
        if kw:
            gs_map[(kw)] = i + 1
    return gs_map


def build_competitors_map(comp_df):
    cmap = {}
    for _, r in comp_df.iterrows():
        kw = str(r["Keyword"]).strip().lower()
        u = normalize_url(str(r["URL"]))
        rn = r["Row Number"]
        cmap.setdefault((kw, u), []).append(rn)
    return cmap


def build_competitors_keyword_map(comp_df, domain):
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
    COMP_COLS = [13, 20, 27]
    FORMULA_COLS = [11, 19, 26, 33, 39, 45]

    matched = []
    new = []

    for _, r in kw_df.iterrows():
        kw = r["Keyword"].strip()
        ru0 = r["Ranking URL"].strip()
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

    # 1) matched: insert rows
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
        time.sleep(2)

        try:
            sheet_id = ws._properties['sheetId']
            copy_requests = []

            for offset in range(len(new_rows)):
                src_row = insert_at - 1
                dst_row = insert_at + offset

                for col in FORMULA_COLS:
                    copy_requests.append({
                        "copyPaste": {
                            "source": {
                                "sheetId": sheet_id,
                                "startRowIndex": src_row - 1,
                                "endRowIndex": src_row,
                                "startColumnIndex": col - 1,
                                "endColumnIndex": col
                            },
                            "destination": {
                                "sheetId": sheet_id,
                                "startRowIndex": dst_row - 1,
                                "endRowIndex": dst_row,
                                "startColumnIndex": col - 1,
                                "endColumnIndex": col
                            },
                            "pasteType": "PASTE_FORMULA",
                            "pasteOrientation": "NORMAL"
                        }
                    })

            if copy_requests:
                retry_on_quota(ws.spreadsheet.batch_update, {"requests": copy_requests})

            batch = []
            for i, url in enumerate(urls):
                cell = rowcol_to_a1(insert_at, COMP_COLS[i])
                batch.append({"range": f"'{ws.title}'!{cell}", "values": [[url]]})

            if batch:
                body = {"valueInputOption": "USER_ENTERED", "data": batch}
                retry_on_quota(ws.spreadsheet.values_batch_update, body)
                logger.info("Batch update (matched): %d ячеек на листе %s", len(batch), ws.title)
                time.sleep(2)

        except RuntimeError as e:
            if "Quota retry failed" in str(e):
                ws.delete_rows(insert_at, insert_at + num_rows - 1)
                logger.warning("Удалили %d пустых строк после quota error (matched)", num_rows)
                continue
            raise

    # 2) new: batch update to empty rows
    batch_new = []
    if new:
        first_empty = find_first_empty_row_in_col_A(ws)
        row_ptr = first_empty

        for item in new:
            kw = item["kw"]
            ru0 = item["ru0"]
            rns = item["rns"]
            urls = item["urls"]

            first_rn = rns[0] if rns else ""

            batch_new.append({
                "range": f"'{ws.title}'!A{row_ptr}:E{row_ptr}",
                "values": [[gs_date, kw, ru0, "", first_rn]]
            })

            for i, url in enumerate(urls):
                cell = rowcol_to_a1(row_ptr, COMP_COLS[i])
                batch_new.append({"range": f"'{ws.title}'!{cell}", "values": [[url]]})

            row_ptr += 1

    if batch_new:
        body = {"valueInputOption": "USER_ENTERED", "data": batch_new}
        retry_on_quota(ws.spreadsheet.values_batch_update, body)
        logger.info("Batch update (new): %d ячеек на листе %s", len(batch_new), ws.title)
        time.sleep(2)


def process_sheet(ws, single_date, date_range, target_date, gs_date):
    project_name = ws.acell("AT1").value
    if not project_name or not project_name.strip():
        logger.warning("Пропускаем лист %s — в AT1 нет project_name", ws.title)
        return
    project_name = project_name.strip()

    logger.info("=== Старт '%s' (project=%s)", ws.title, project_name)

    kws = fetch_keywords(project_name, single_date, HEADERS)
    if not kws:
        logger.warning("Нет ключей для %s, пропускаем", project_name)
        return

    fst, snd = build_sheet_data(project_name, kws, date_range, target_date)

    save_to_excel(fst, snd, EXCEL_FILE)

    safe_title = ws.title.replace(" ", "_").replace("/", "_")
    copy_name = f"keywords_full_data_{safe_title}.xlsx"
    save_to_excel(fst, snd, copy_name)
    logger.info("  → дамп Excel для отладки: %s", copy_name)

    kw_df = pd.read_excel(EXCEL_FILE, sheet_name="Keywords")
    comp_df = pd.read_excel(EXCEL_FILE, sheet_name="Competitors")

    gs_map = build_gs_map(ws)
    comp_map = build_competitors_map(comp_df)
    comp_kw_map = build_competitors_keyword_map(comp_df, project_name)

    update_google_sheet(ws, kw_df, comp_map, gs_map, comp_kw_map, gs_date)
    logger.info("=== Готово: %s", ws.title)


def main():
    logger.info("START keyword_kz")
    logger.info("Using RATE_LIMIT_DELAY=%s", RATE_LIMIT_DELAY)
    logger.info("Spreadsheet=%s", SPREADSHEET_URL)

    single_date, date_range, target_date, gs_date = get_dates()

    global HEADERS
    HEADERS = {"Authorization": f"Bearer {TOKEN}"}

    analysis_sheets = find_analysis_sheets(SPREADSHEET_URL, CREDS_FILE)
    logger.info("Found %d analysis sheets", len(analysis_sheets))

    for ws in analysis_sheets:
        try:
            logger.info("Processing sheet: %s", ws.title)
            process_sheet(ws, single_date, date_range, target_date, gs_date)
            time.sleep(3)
        except Exception as e:
            logger.error("Ошибка обработки листа %s: %s", ws.title, e, exc_info=True)

    logger.info("Вся обработка завершена.")


if __name__ == "__main__":
    main()
