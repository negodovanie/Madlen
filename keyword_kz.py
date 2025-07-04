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
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/15SRs1ZV5204S7e-_3nEfWNbuaX3Ssp8pegVGISoIXUs/edit?gid=45369749#gid=45369749"
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
    twoweek = datetime.today() - timedelta(days=14)
    single     = yesterday.strftime("%Y-%m-%d")        # для API-запроса dateRange
    twoweek = twoweek.strftime("%Y-%m-%d")
    date_range = f"{twoweek}.{single}"
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
    """
    first  — строки для листа Keywords,
    second — строки для листа Competitors.

    Если в history нет блока по target_date (в формате "Jun 30, 2025"),
    берём блок с максимальной датой (тоже в том же формате).
    """
    first, second = [], []

    for item in keywords_data:
        kid  = item["id"]
        attr = item.get("attributes", {})
        kw   = attr.get("kw")
        ru   = attr.get("rankingurl")
        ua   = attr.get("updated_at")
        first.append({
            "ID": kid,
            "Keyword": kw,
            "Ranking URL": ru,
            "Updated At": ua
        })

        history = fetch_competitors_history(project_name, kid, date_range)

        # 1) Сначала пытаемся найти точный блок по target_date
        chosen = next(
            (blk for blk in history if blk.get("date") == target_date),
            None
        )

        # 2) Если не нашли и history непустая — берём самый свежий по дате
        if chosen is None and history:
            # парсим дату вида "Jun 30, 2025"
            def parse_blk_date(b):
                try:
                    return datetime.strptime(b.get("date",""), "%b %d, %Y")
                except Exception:
                    return datetime.min

            chosen = max(history, key=parse_blk_date)
            logger.info(
                "(%s:%s) для даты %s данных нет, взяли самый свежий блок %s",
                project_name, kid, target_date, chosen.get("date")
            )

        # 3) Если в `chosen` что-то есть — раскладываем его результаты
        if chosen:
            ctr = 1
            for res in chosen.get("results", []):
                second.append({
                    "ID":         kid,
                    "Keyword":    kw,
                    "URL":        res.get("url"),
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
    CITY_COL  = 4       # D
    COMP_COLS = [13,20,27]  # M, T, AA
    FORMULA_COLS = [11, 19, 26, 33, 39, 45]  # K, S, Z, AG, AM, AS

    # Собираем два списка: matched — те, где нашли gs_row и есть rns
    matched = []
    new     = []

    for _, r in kw_df.iterrows():
        kw   = r["Keyword"].strip()
        ru0  = r["Ranking URL"].strip()
        key  = (kw.lower(), normalize_url(ru0))
        gs_row = gs_map.get(key)
        rns     = comp_map.get(key, [])
        urls    = comp_kw_map.get(kw.lower(), [])[:3]

        if gs_row is not None and rns:
            matched.append({
                "kw":     kw,
                "ru0":    ru0,
                "gs_row": gs_row,
                "rns":    rns,
                "urls":   urls
            })
        else:
            new.append({
                "kw":  kw,
                "ru0": ru0,
                "rns": rns,
                "urls":urls
            })

    batch = []
    shift = 0

    # 1) Обработка найденных: INSERT rows сразу под каждой найденной строкой
    for item in matched:
        kw      = item["kw"]
        ru0     = item["ru0"]
        gs_row  = item["gs_row"]
        rns     = item["rns"]
        urls    = item["urls"]

        # город из колонки D той же найденной строки
        prev_city = ws.cell(gs_row, CITY_COL).value or ""

        # формируем список строк для вставки
        new_rows = [[gs_date, kw, ru0, prev_city, rn] for rn in rns]

        insert_at = gs_row + shift + 1
        retry_on_quota(ws.insert_rows, new_rows, row=insert_at)
        shift += len(new_rows)

        sheet_id = ws._properties['sheetId']
        copy_requests = []

        for offset in range(len(new_rows)):
            src_row = insert_at + offset - 1  # строка-источник
            dst_row = insert_at + offset  # строка-назначения

            for col in FORMULA_COLS:
                copy_requests.append({
                    "copyPaste": {
                        "source": {
                            "sheetId": sheet_id,
                            "startRowIndex": src_row - 1,
                            "endRowIndex": src_row,  # не включительно
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

        # в каждую вставленную строку пишем конкурентные URL
        # (держим только первые len(urls) строкок, если хотим в каждую по одному URL)
        # потом вставляем первые N URL-ов, где N = min(len(rns), len(urls))
        for i, url in enumerate(urls):
            cell = rowcol_to_a1(insert_at, COMP_COLS[i])
            batch.append({
                "range": f"'{ws.title}'!{cell}",
                "values": [[url]]
            })

    # 2) Обработка новых: вписываем в первую пустую строку по A, затем по следующей и т.д.
    if new:
        first_empty = find_first_empty_row_in_col_A(ws)
        row_ptr     = first_empty

        for item in new:
            kw   = item["kw"]
            ru0  = item["ru0"]
            rns  = item["rns"]
            urls = item["urls"]

            first_rn = rns[0] if rns else ""

            # A–E
            batch.append({
                "range": f"'{ws.title}'!A{row_ptr}:E{row_ptr}",
                "values": [[gs_date, kw, ru0, "", first_rn]]
            })

            # M/T/AA
            for i, url in enumerate(urls):
                cell = rowcol_to_a1(row_ptr, COMP_COLS[i])
                batch.append({"range": f"'{ws.title}'!{cell}", "values": [[url]]})

            # «зарезервируем» эту строку, чтобы не переписать её
            gs_map[(kw.lower(), normalize_url(ru0))] = row_ptr
            row_ptr += 1

    # 3) Выполняем batch-апдейт всех URL (и A–E для новых)
    if batch:
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