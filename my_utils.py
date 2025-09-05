import json
import gspread
import time
import os

def load_api_tokens():
    # Путь к файлу относительно корня проекта
    tokens_path = os.path.join(os.path.dirname(__file__), 'tokens.json')
    with open(tokens_path, 'r', encoding='utf-8') as f:
        return json.load(f)
    
def safe_open_spreadsheet(title, retries=5, delay=5):
    """
    Пытается открыть таблицу с повторными попытками при APIError 503.
    """
    gc = gspread.service_account(filename=r'C:\Users\123\Desktop\cards_flow_git\creds.json')
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


