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
LOG_FILE         = "keyword_kz.log"
TOKEN            = "kPQGRMFx7JYdJ3mqQyqGF62CRtPGKTb7"  # НЕ ТРОГАЮ
EXCEL_FILE       = "keywords_full_data.xlsx"
CREDS_FILE       = "level-landing-195008-a8940ac6b2ab.json"
SPREADSHEET_URL  = "https://docs.google.com/spreadsheets/d/1tvLCWC5WhBnQAFQpoJsVUXnjtDlzjRbJwzmsYgM8b8c/edit?gid=639673523#gid=639673523"
RATE_LIMIT_DELAY = 2  # секунда между запросами

# Антизависание: таймаут HTTP (можно переопределить env, но по умолчанию адекватно)
HTTP_TIMEOUT     = float(os.getenv("HTTP_TIMEOUT", "60"))

# Дампы/отладка: чтобы НЕ сломать текущее поведение, по умолчанию True.
# Если захочешь выключить: DEBUG_DUMP=0
DEBUG_DUMP       = os.getenv("DEBUG_DUMP", "1") not in ("0", "false", "False", "")

logging.basicConfig(
    filename=LOG_FILE,
    filemode='w',
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Пишем также в stdout, чтобы в GitHub Actions был прогресс
_console = logging.StreamHandler(sys.stdout)
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_console)


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
                logger.warning("Quota exceeded (attempt %d/%d). Ждём %s сек и пробуем снова...",
                               attempt, max_attempts, delay)
                time.sleep(delay)
                delay *= 2
                continue
            raise
    logger.error("Не удалось выполнить %s после %d попыток — выходим", func.__name__, max_attempts)
    raise RuntimeError(f"Quota retry failed: {func.__name__}")


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
    url = f"https://app.keyword.com/api/v2/groups/{project_name}/keywords/"
    params = {"per_page": 250, "page": 1, "date": single_date}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logger.exception("(%s) Ошибка HTTP первого запроса: %s", project_name, e)
        return []

    if resp.status_code != 200:
        logger.error("(%s) Ошибка первого запроса: %s %s",
                     project_name, resp.status_code, resp.text)
        return []
    return resp.json().get("data", [])


def fetch_competitors_history(project_name, keyword_id, date_range):
    url = f"https://app.keyword.com/api/v2/metrics/{project_name}/competitors/{keyword_id}/history"
    params = {"dateRange": date_range}
    logger.info("→ fetch_competitors_history: project=%s keyword_id=%s", project_name, keyword_id)

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logger.exception("(%s) Ошибка HTTP-запроса истории для %s: %s", project_name, keyword_id, e)
        return []

    logger.info("← fetch_competitors_history: status=%s", resp.status_code)

    # логируем первые 500 символов тела (debug)
    snippet = resp.text[:500].replace("\n", " ")
    logger.debug("← body snippet: %s", snippet)

    # сбросить полный JSON в файл для отладки (как было), но можно выключить env DEBUG_DUMP=0
    if DEBUG_DUMP:
        dump_dir = "debug_history"
        os.makedirs(dump_dir, exist_ok=True)
        fname = os.path.join(dump_dir, f"{project_name}_{keyword_id}_{date_range}.json")
        try:
            with open(fname, "w", encoding="utf-8") as f:
                f.write(resp.text)
            logger.info("  → полный ответ сохранён в %s", fname)
        except Exception as e:
            logger.error("Не смог сохранить ответ в файл %s: %s", fname, e)

    if resp.status_code != 200:
        logger.error("(%s) Ненулевой статус %s для history %s: %s",
                     project_name, resp.status_code, keyword_id, resp.text)
        return []

    try:
        data = resp.json().get("data", [])
    except Exception as e:
        logger.exception("Ошибка разбора JSON history для %s/%s: %s", project_name, keyword_id, e)
        return []

    logger.info("→ history data blocks: %d", len(data))
    return data


def build_sheet_data(project_name, keywords_data, date_range, target_date):
    first, second = [], []

    for idx, item in enumerate(keywords_data, start=1):
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
            logger.info(
                "(%s:%s) для даты %s данных нет, взяли самый свежий блок %s",
                project_name, kid, target_date, chosen.get("date")
            )

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

        if idx % 25 == 0:
            logger.info("(%s) обработано ключей: %d/%d", project_name, idx, len(keywords_data))

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
    """
    Строит словарь keyword -> row_number из столбца B.
    """
    col_b = ws.col_values(2)  # B
    gs_map = {}
    for i in range(1, len(col_b)):  # пропускаем заголовок
        kw = col_b[i].strip().lower()
        if kw:
            gs_map[kw] = i + 1
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
            logger.debug("  → на %s пропускаем конкурент %s, домен совпадает", kw, url)
            continue

        if any(sub in url_lc for sub in forbidden_substrings):
            logger.debug("  → на %s пропускаем конкурент %s, запрещённая подстрока", kw, url)
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

        gs_row = gs_map.get(kw_lc)  # ищем только по keyword
        key = (kw_lc, normalize_url(ru0))
        rns = comp_map.get(key, [])
        urls = comp_kw_map.get(kw_lc, [])[:3]

        if gs_row is not None and rns:
            matched.append({"kw": kw, "ru0": ru0, "gs_row": gs_row, "rns": rns, "urls": urls})
        else:
            new.append({"kw": kw, "ru0": ru0, "rns": rns, "urls": urls})

    matched.sort(key=lambda x: x["gs_row"], reverse=True)

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
                logger.warning("Удалили %d пустых строк после quota error на заполнении (matched)", num_rows)
                continue
            raise

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

            # фикс: gs_map у нас только по keyword
            gs_map[kw.lower()] = row_ptr
            row_ptr += 1

    if batch_new:
        body = {"valueInputOption": "USER_ENTERED", "data": batch_new}
        retry_on_quota(ws.spreadsheet.values_batch_update, body)
        logger.info("Batch update: %d ячеек на листе %s", len(batch_new), ws.title)
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

    # дампы Excel оставляю как были, но можно выключить DEBUG_DUMP=0
    if DEBUG_DUMP:
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
    single_date, date_range, target_date, gs_date = get_dates()
    global HEADERS
    HEADERS = {"Authorization": f"Bearer {TOKEN}"}

    analysis_sheets = find_analysis_sheets(SPREADSHEET_URL, CREDS_FILE)
    for ws in analysis_sheets:
        try:
            process_sheet(ws, single_date, date_range, target_date, gs_date)
            time.sleep(3)
        except Exception as e:
            logger.error("Ошибка обработки листа %s: %s", ws.title, e, exc_info=True)

    logger.info("Вся обработка завершена.")


if __name__ == "__main__":
    main()
