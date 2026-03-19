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
        print(f"[Попытка {attempt}] открыть доступ к таблице '{title}'")
        
        try:
            spreadsheet = gc.open(title)
            print(f"✅ Таблица '{title}' успешно открыта")
            return spreadsheet
            
        except gspread.exceptions.APIError as e:
            error_code = e.response.status_code if hasattr(e, 'response') else None
            print(f"⚠️ [Попытка {attempt}/{retries}] APIError {error_code}: {e}")
            
            if error_code == 503:
                if attempt < retries:
                    print(f"⏳ Ожидание {delay} секунд перед повторной попыткой...")
                    time.sleep(delay)
                    # Увеличиваем задержку для следующей попытки (exponential backoff)
                    delay *= 2
                else:
                    print("❌ Все попытки исчерпаны")
                    raise
            else:
                # Другие ошибки API (403, 404 и т.д.) - не повторяем
                raise
                
        except gspread.SpreadsheetNotFound:
            print(f"❌ Таблица '{title}' не найдена")
            raise
            
        except Exception as e:
            print(f"⚠️ [Попытка {attempt}/{retries}] Неожиданная ошибка: {e}")
            if attempt < retries:
                print(f"⏳ Ожидание {delay} секунд...")
                time.sleep(delay)
                delay *= 2
            else:
                raise RuntimeError(f"Не удалось открыть таблицу '{title}' после {retries} попыток.")

# # Использование
# try:
#     spreadsheet = safe_open_spreadsheet("Название вашей таблицы")
#     worksheet = spreadsheet.worksheet("Настройки автопилота")
#     data = worksheet.get_all_values()
#     print(f"✅ Получено {len(data)} строк данных")
    
# except Exception as e:
#     print(f"💥 Ошибка: {e}")


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