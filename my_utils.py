import json
import gspread
import time

# Функция для загрузки API токенов из файла tokens.json
def load_api_tokens():
    with open(r'C:\Users\123\Desktop\cards_flow_git\tokens.json', encoding= 'utf-8') as f:
        tokens = json.load(f)
        return tokens
    
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


