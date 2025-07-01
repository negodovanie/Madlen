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
from gspread.exceptions import APIError

# ---------------- НАСТРОЙКИ ------------------
LOG_FILE        = "keyword_kz.log"
TOKEN           = "kPQGRMFx7JYdJ3mqQyqGF62CRtPGKTb7"
EXCEL_FILE      = "keywords_full_data.xlsx"
CREDS_FILE      = "level-landing-195008-a8940ac6b2ab.json"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1Wfmzxh4ixO-bBFTpB0k-qVZzU1KXruH97ZMoXqhiedw/edit?gid=2042365344#gid=2042365344"
RATE_LIMIT_DELAY= 1  # секунда между запросами

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
            # иные APIError пробрасываем
            raise
    # после всех попыток
    logger.error("Не удалось выполнить %s после %d попыток — выходим", func.__name__, max_attempts)
    raise APIError(f"Quota retry failed: {func.__name__}")


def find_first_empty_row_in_col_A(ws):
    # возвращает индекс первой строки, где в колонке A пусто
    col_a = ws.col_values(1)  # все непустые значения колонки A
    for idx, val in enumerate(col_a, start=1):
        if not str(val).strip():
            return idx
    # если ни одной пустой не нашли — вставляем сразу после всех непустых
    return len(col_a) + 1

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
    # вчерашний день
    yesterday  = datetime.today() - timedelta(days=1)
    single     = yesterday.strftime("%Y-%m-%d")        # для API-запроса dateRange
    date_range = f"{single}.{single}"
    # target_date теперь — в формате "Jun 30, 2025", как у API:
    target     = yesterday.strftime("%b %d, %Y")
    # gs_date для Google-таблицы
    gs_date    = yesterday.strftime("%d.%m.%Y")
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
    logger.info("→ fetch_competitors_history: url=%s params=%s", url, params)

    try:
        resp = requests.get(url, headers=HEADERS, params=params)
    except Exception as e:
        logger.exception("(%s) Ошибка HTTP-запроса истории для %s: %s", project_name, keyword_id, e)
        return []

    logger.info("← fetch_competitors_history: status=%s", resp.status_code)
    # логируем первые 500 символов тела и сохраняем весь ответ в файл для отладки
    snippet = resp.text[:500].replace("\n", " ")
    logger.debug("← body snippet: %s", snippet)

    # сбросить полный JSON в файл
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

    data = []
    try:
        data = resp.json().get("data", [])
    except Exception as e:
        logger.exception("Ошибка разбора JSON history для %s/%s: %s", project_name, keyword_id, e)

    logger.info("→ history data blocks: %d", len(data))
    return data


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
            # blk['date'] теперь сравнивается с target_date == "YYYY-MM-DD"
            if blk.get("date") == target_date:
                for r in blk.get("results", []):
                    second.append({
                        "ID": kid,
                        "Keyword": kw,
                        "URL": r.get("url"),
                        "Row Number": ctr
                    })
                    ctr += 1
        time.sleep(RATE_LIMIT_DELAY)
    return first, second


def save_to_excel(first_sheet, second_sheet, filename):
    with pd.ExcelWriter(filename) as w:
        pd.DataFrame(first_sheet).to_excel(w, sheet_name="Keywords", index=False)
        pd.DataFrame(second_sheet).to_excel(w, sheet_name="Competitors", index=False)
    logger.info("Экспорт в Excel готов: %s", filename)



def normalize_url(u: str) -> str:
    """Убираем http(s)://, www. и слэши в конце, приводим к lowercase."""
    u = u.strip().lower()
    u = re.sub(r'^https?://', '', u)
    u = re.sub(r'^www\.', '', u)
    return u.rstrip('/')

def read_google_sheet(worksheet):
    """Читает весь лист и возвращает (headers, rows)."""
    all_values = worksheet.get_all_values()
    if not all_values:
        return [], []
    return all_values[0], all_values[1:]


def build_gs_map(ws):
    """
    Строит словарь (keyword, url) -> номер строки из столбцов B и C листа ws.
    Пропускает первую строку (заголовок).
    """
    col_b = ws.col_values(2)   # B
    col_c = ws.col_values(3)   # C
    n = min(len(col_b), len(col_c))
    gs_map = {}
    # i=0 — это шапка, начинаем с i=1 → row = i+1
    for i in range(1, n):
        kw = col_b[i].strip().lower()
        ru = normalize_url(col_c[i])
        if kw and ru:
            gs_map[(kw, ru)] = i + 1
    return gs_map


def build_competitors_map(comp_df):
    """
    Строит словарь (keyword, normalized_url) -> список Row Number из Competitors DataFrame.
    """
    cmap = {}
    for _, r in comp_df.iterrows():
        kw = str(r["Keyword"]).strip().lower()
        u  = normalize_url(str(r["URL"]))
        rn = r["Row Number"]
        cmap.setdefault((kw, u), []).append(rn)
    return cmap


def build_competitors_keyword_map(comp_df, domain):
    """
    Строит словарь Keyword -> список чужих URL (без текущего домена).
    Фильтрует url, чтобы исключить домен текущего проекта.
    """
    kw_map = {}
    for _, r in comp_df.iterrows():
        kw  = str(r["Keyword"]).strip().lower()
        url = str(r["URL"]).strip()
        if domain not in url:
            kw_map.setdefault(kw, []).append(url)
        else:
            logger.debug("  → на %s пропускаем конкурент %s, домен совпадает", kw, url)
    return kw_map


def update_google_sheet(ws, kw_df, comp_map, gs_map, comp_kw_map, gs_date):
    # Подготовка констант
    CITY_COL = 4  # D
    COLS = [13, 20, 27]  # M, T, AA

    insert_operations = []
    append_rows = []
    batch = []
    shift = 0

    for _, r in kw_df.iterrows():
        kw = str(r["Keyword"]).strip()
        ru0 = str(r["Ranking URL"]).strip()
        ru = normalize_url(ru0)
        key = (kw.lower(), ru)
        gs_row = gs_map.get(key)
        rns = comp_map.get(key, [])
        urls = comp_kw_map.get(kw.lower(), [])[:3]

        if gs_row is not None:
            # Найдена существующая строка, вставляем новые competitor-строки сразу под ней
            prev_city = ws.cell(gs_row, CITY_COL).value or ""
            new_rows = []
            for i, rn in enumerate(rns, start=1):
                new_rows.append([gs_date, kw, ru0, prev_city, rn])
            insert_pos = gs_row + shift + 1
            insert_operations.append((insert_pos, new_rows))
            shift += len(new_rows)

            # Batch-запросы на заполнение колонок M,T,AA для первой вставленной строки
            for col_idx, url in zip(COLS, urls):
                batch.append({
                    "range": f"'{ws.title}'!{rowcol_to_a1(insert_pos, col_idx)}",
                    "values": [[url]]
                })
        else:
            # Новая ключ-фраза → в конец
            append_rows.append([gs_date, kw, ru0, "", rns[0] if rns else ""])
            # номер будущей строки определим позже

    # 1) В середине листа вставляем
    for pos, rows_to_insert in sorted(insert_operations, key=lambda x: x[0]):
        retry_on_quota(ws.insert_rows, rows_to_insert, row=pos)

    # 2) В конец листа добавляем новые
    if append_rows:
        first_empty = find_first_empty_row_in_col_A(ws)
        retry_on_quota(
            ws.insert_rows,
            append_rows,
            row=first_empty,
            value_input_option="USER_ENTERED"
        )
        # Заполняем конкурентов для append_rows
        for i, r in enumerate(append_rows):
            row_num = first_empty + i
            kw = r[1]
            urls = comp_kw_map.get(kw.lower(), [])[:3]
            for col_idx, url in zip(COLS, urls):
                batch.append({
                    "range": f"'{ws.title}'!{rowcol_to_a1(row_num, col_idx)}",
                    "values": [[url]]
                })

    # 3) Отправляем batch-запрос
    if batch:
        # DEBUG: сохраним batch в json, чтобы посмотреть, какие ranges и values у нас получились
        import json, os
        dump_file = f"batch_{ws.title.replace(' ', '_')}.json"
        with open(dump_file, "w", encoding="utf-8") as f:
            json.dump(batch, f, ensure_ascii=False, indent=2)
        logger.info("  → дамп batch-запроса: %s", dump_file)

        body = {"valueInputOption": "USER_ENTERED", "data": batch}
        retry_on_quota(ws.spreadsheet.values_batch_update, body)
        logger.info("Batch update: %d ячеек на листе %s", len(batch), ws.title)


def process_sheet(ws, single_date, date_range, target_date, gs_date):
    # Безопасно читаем project_name из AT1
    project_name = ws.acell("AT1").value
    if not project_name or not project_name.strip():
        logger.warning("Пропускаем лист %s — в AT1 нет project_name", ws.title)
        return
    project_name = project_name.strip()
    logger.info("=== Старт '%s' (project=%s)", ws.title, project_name)

    # Получаем ключи
    kws = fetch_keywords(project_name, single_date, HEADERS)
    if not kws:
        logger.warning("Нет ключей для %s, пропускаем", project_name)
        return

    # Создаём Excel
    fst, snd = build_sheet_data(project_name, kws, date_range, target_date)
    save_to_excel(fst, snd, EXCEL_FILE)

    # после сохранения в keywords_full_data.xlsx
    safe_title = ws.title.replace(" ", "_").replace("/", "_")
    copy_name = f"keywords_full_data_{safe_title}.xlsx"
    save_to_excel(fst, snd, copy_name)
    logger.info("  → дамп Excel для отладки: %s", copy_name)

    # Читаем из Excel
    kw_df   = pd.read_excel(EXCEL_FILE, sheet_name="Keywords")
    comp_df = pd.read_excel(EXCEL_FILE, sheet_name="Competitors")

    # Создаём карты для быстрого поиска и фильтрации
    _, rows      = read_google_sheet(ws)
    gs_map       = build_gs_map(ws)
    comp_map     = build_competitors_map(comp_df)
    comp_kw_map  = build_competitors_keyword_map(comp_df, project_name)

    # Обновляем лист
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
        except Exception as e:
            logger.error(f"Ошибка обработки листа {ws.title}: {e}", exc_info=True)

    logger.info("Вся обработка завершена.")


if __name__ == "__main__":
    main()