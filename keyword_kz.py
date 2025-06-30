import logging
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import rowcol_to_a1

# ----------------– НАСТРОЙКИ ------------------
LOG_FILE        = "keyword_kz.log"
TOKEN           = "kPQGRMFx7JYdJ3mqQyqGF62CRtPGKTb7"
EXCEL_FILE      = "keywords_full_data.xlsx"
CREDS_FILE      = "pythonbider-680ebdbe164a.json"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1lm5EwmprNcXNO7mENe83hkWe9vKq9TrsotsMkdmu05Y/edit?gid=45369749#gid=45369749"
RATE_LIMIT_DELAY= 0.5  # секунда между запросами

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def authorize_gspread(creds_file):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    return gspread.authorize(creds)


def find_analysis_sheets(spreadsheet_url, creds_file):
    """Возвращает список Worksheet, в названии которых есть 'анализ'."""
    gc = authorize_gspread(creds_file)
    sh = gc.open_by_url(spreadsheet_url)
    return [ws for ws in sh.worksheets() if "анализ" in ws.title.lower()]


def get_dates():
    today = datetime.today()
    single     = today.strftime("%Y-%m-%d")
    date_range = f"{single}.{single}"
    target     = today.strftime("%b %d, %Y")
    gs_date    = today.strftime("%d.%m.%Y")
    return single, date_range, target, gs_date


def fetch_keywords(project_name, single_date, headers):
    url = f"https://app.keyword.com/api/v2/groups/{project_name}/keywords/"
    params = {"per_page": 250, "page": 1, "date": single_date}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        logger.error("(%s) Ошибка первого запроса: %s %s",
                     project_name, resp.status_code, resp.text)
        return []
    return resp.json().get("data", [])


def fetch_competitors_history(project_name, keyword_id, date_range):
    url = f"https://app.keyword.com/api/v2/metrics/{project_name}/competitors/{keyword_id}/history"
    params = {"dateRange": date_range}
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code != 200:
        logger.error("(%s) Ошибка истории для %s: %s %s",
                     project_name, keyword_id, resp.status_code, resp.text)
        return []
    return resp.json().get("data", [])


def build_sheet_data(project_name, keywords_data, date_range, target_date):
    first, second = [], []
    for item in keywords_data:
        kid  = item["id"]
        attr = item.get("attributes", {})
        kw   = attr.get("kw")
        ru   = attr.get("rankingurl")
        ua   = attr.get("updated_at")
        first.append({"ID": kid, "Keyword": kw, "Ranking URL": ru, "Updated At": ua})

        history = fetch_competitors_history(project_name, kid, date_range)
        ctr = 1
        for blk in history:
            if blk.get("date") == target_date:
                for r in blk.get("results", []):
                    second.append({
                        "ID": kid, "Keyword": kw, "URL": r.get("url"), "Row Number": ctr
                    })
                    ctr += 1
        time.sleep(RATE_LIMIT_DELAY)
    return first, second


def save_to_excel(first_sheet, second_sheet, filename):
    with pd.ExcelWriter(filename) as w:
        pd.DataFrame(first_sheet).to_excel(w, sheet_name="Keywords", index=False)
        pd.DataFrame(second_sheet).to_excel(w, sheet_name="Competitors", index=False)
    logger.info("Экспорт в Excel готов: %s", filename)


# =====================================================================
# Вот теперь все те функции, которые вы «пропустили» в предыдущем варианте
# =====================================================================

def read_google_sheet(worksheet):
    """Читает весь лист и возвращает (headers, rows)."""
    all_values = worksheet.get_all_values()
    if not all_values:
        return [], []
    return all_values[0], all_values[1:]


def build_gs_map(rows):
    """
    Строит словарь (keyword, url) -> номер строки.
    rows — список списков (значения строк без заголовка).
    """
    gs_map = {}
    for idx, row in enumerate(rows, start=2):
        if len(row) >= 3:
            key = (row[1].strip().lower(), row[2].strip().lower())
            gs_map[key] = idx
    return gs_map


def build_competitors_map(comp_df):
    """
    Строит словарь URL -> список Row Number из Competitors DataFrame.
    """
    cmap = {}
    for _, r in comp_df.iterrows():
        u = str(r["URL"]).strip().lower()
        rn = r["Row Number"]
        cmap.setdefault(u, []).append(rn)
    return cmap


def build_competitors_keyword_map(comp_df):
    """
    Строит словарь Keyword -> список чужих URL (без текущего домена).
    """
    kw_map = {}
    for _, r in comp_df.iterrows():
        kw  = str(r["Keyword"]).strip().lower()
        url = str(r["URL"]).strip()
        if "eco-service.kz" not in url:  # или любой другой фильтр
            kw_map.setdefault(kw, []).append(url)
    return kw_map


def update_google_sheet(ws, kw_df, comp_map, gs_map, comp_kw_map, gs_date):
    COLS     = {0: 13, 1: 20, 2: 27}  # M, T, AA
    CITY_COL = 4
    batch    = []
    shift    = 0
    sheet_name = ws.title

    for _, row in kw_df.iterrows():
        kw   = str(row["Keyword"]).strip()
        ru   = str(row["Ranking URL"]).strip()
        key  = (kw.lower(), ru.lower())
        gs_i = gs_map.get(key)
        rns  = comp_map.get(ru.lower(), [])

        if gs_i and rns:
            prev_city = ws.cell(gs_i, CITY_COL).value or ""
            for i, rn in enumerate(rns):
                tgt = gs_i + 1 + shift + i
                new = [gs_date, kw, ru, prev_city, rn]
                ws.insert_rows([new], row=tgt)
                urls = comp_kw_map.get(kw.lower(), [])[:3]
                for j, u in enumerate(urls):
                    batch.append({
                        "range": f"'{sheet_name}'!{rowcol_to_a1(tgt, COLS[j])}",
                        "values": [[u]]
                    })
            shift += len(rns)
        else:
            new = [gs_date, kw, ru, "", rns[0] if rns else ""]
            ws.append_rows([new], value_input_option="USER_ENTERED")
            last = len(ws.get_all_values())
            urls = comp_kw_map.get(kw.lower(), [])[:3]
            for j, u in enumerate(urls):
                batch.append({
                    "range": f"'{sheet_name}'!{rowcol_to_a1(last, COLS[j])}",
                    "values": [[u]]
                })

    if batch:
        body = {"valueInputOption": "USER_ENTERED", "data": batch}
        ws.spreadsheet.values_batch_update(body)
        logger.info("Batch-update %d ячеек на листе %s", len(batch), sheet_name)


# =====================================================================
# Основной процесс обработки одного листа
# =====================================================================

def process_sheet(ws, single_date, date_range, target_date, gs_date):
    # 1) читаем PROJECT_NAME из ячейки AT1
    project_name = ws.acell("AT1").value.strip()
    logger.info("=== Старт '%s' (project=%s)", ws.title, project_name)

    # 2) получаем ключи
    kws = fetch_keywords(project_name, single_date, HEADERS)
    if not kws:
        logger.warning("Нет ключей для %s, пропускаем", project_name)
        return

    # 3) создаём Excel
    fst, snd = build_sheet_data(project_name, kws, date_range, target_date)
    save_to_excel(fst, snd, EXCEL_FILE)

    # 4) читаем из Excel
    kw_df   = pd.read_excel(EXCEL_FILE, sheet_name="Keywords")
    comp_df = pd.read_excel(EXCEL_FILE, sheet_name="Competitors")

    # 5) строим карты
    _, rows      = read_google_sheet(ws)
    gs_map       = build_gs_map(rows)
    comp_map     = build_competitors_map(comp_df)
    comp_kw_map  = build_competitors_keyword_map(comp_df)

    # 6) обновляем лист
    update_google_sheet(ws, kw_df, comp_map, gs_map, comp_kw_map, gs_date)
    logger.info("=== Готово: %s", ws.title)


def main():
    single_date, date_range, target_date, gs_date = get_dates()
    global HEADERS
    HEADERS = {"Authorization": f"Bearer {TOKEN}"}

    analysis_sheets = find_analysis_sheets(SPREADSHEET_URL, CREDS_FILE)
    for ws in analysis_sheets:
        process_sheet(ws, single_date, date_range, target_date, gs_date)

    logger.info("Вся обработка завершена.")


if __name__ == "__main__":
    main()