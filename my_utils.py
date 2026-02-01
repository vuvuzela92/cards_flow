import json
import gspread
import time
import os
from gspread.utils import rowcol_to_a1

def load_api_tokens():
    # Путь к файлу относительно корня проекта
    tokens_path = os.path.join(os.path.dirname(__file__), 'tokens.json')
    with open(tokens_path, 'r', encoding='utf-8') as f:
        return json.load(f)
    
def safe_open_spreadsheet(title, retries=5, delay=5):
    """
    Пытается открыть таблицу с повторными попытками при APIError 503.
    """
    gc = gspread.service_account(filename=os.path.join(os.path.dirname(__file__), 'creds.json'))
    for attempt in range(1, retries + 1):
        print(f"[Попытка {attempt} октрыть доступ к таблице")
        try:
            return gc.open(title)
        except APIError as e:
            if "503" in str(e):
                print(f"[Попытка {attempt}/{retries}] APIError 503 — повтор через {delay} сек.")
                time.sleep(delay)
            else:
                raise  # если ошибка не 503 — пробрасываем дальше
    raise RuntimeError(f"Не удалось открыть таблицу '{title}' после {retries} попыток.")


def insert_multiple_columns(sheet, headers_loc: int, headers: list, data_loc: int, data_matrix: list):
    """
    Вставляет сразу несколько колонок в таблицу.
    
    :param sheet: Лист таблицы
    :param headers_loc: Номер строки заголовков
    :param headers: Список заголовков колонок, которые нужно обновить
    :param data_loc: Строка начала вставки (например, 2)
    :param data_matrix: Список списков со значениями по колонкам. Формат:
                        [
                            [val1_col1, val1_col2, val1_col3],
                            [val2_col1, val2_col2, val2_col3],
                            ...
                        ]
    """
    sheet_headers = sheet.row_values(headers_loc)
    
    col_indices = []
    for header in headers:
        try:
            col_index = sheet_headers.index(header) + 1
            col_indices.append(col_index)
        except ValueError:
            raise ValueError(f"Заголовок {header} не найден")
    
    for i, col_index in enumerate(col_indices):
        # Формируем данные по конкретной колонке
        col_data = [[row[i]] for row in data_matrix]
        
        start_cell = rowcol_to_a1(row=data_loc, col=col_index)
        end_cell = rowcol_to_a1(row=data_loc + len(data_matrix) - 1, col=col_index)
        
        cell_range = f"{start_cell}:{end_cell}"
        sheet.update(cell_range, col_data)
        print(f'Данные {headers} обновлены')