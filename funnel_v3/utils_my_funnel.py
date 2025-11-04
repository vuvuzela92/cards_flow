import os
import json
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import gspread
from time import time
from calendar import monthrange
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from utils_sql import create_connection, get_db_table
from gspread_dataframe import set_with_dataframe
from datetime import datetime
import os


# === НАСТРОЙКА ЛОГИРОВАНИЯ ===
logger = logging.getLogger("funnel_logger")
logger.setLevel(logging.INFO)

# Форматтер
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')

# 1. Хендлер в ФАЙЛ
file_handler = logging.FileHandler('funnel.log', encoding='utf-8')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# 2. Хендлер в КОНСОЛЬ
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

# Добавляем оба хендлера
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def load_api_tokens():
    # Путь к файлу относительно корня проекта
    tokens_path = os.path.join(os.path.dirname(__file__), 'tokens.json')
    with open(tokens_path, 'r', encoding='utf-8') as f:
        return json.load(f)
    

def get_first_and_last_day(month: int):
    """
    Возвращает первую и последнюю дату месяца по номеру месяца и году.
    
    Args:
        year (int): Год (например, 2025)
        month (int): Месяц (1–12)
    
    Returns:
        tuple: (первая_дата, последняя_дата) в формате datetime.date
    """
    year = datetime.now().year

    # Первое число месяца
    first_day = datetime(year, month, 1).date()
    
    # Последнее число месяца — используем monthrange
    last_day_num = monthrange(year, month)[1]
    last_day = datetime(year, month, last_day_num).date()
    
    return first_day, last_day

    
def safe_open_spreadsheet(title, retries=5, delay=5):
    """
    Пытается открыть таблицу с повторными попытками при APIError 503.
    """
    gc = gspread.service_account(filename=os.path.join(os.path.dirname(__file__), 'creds.json'))
    
    for attempt in range(1, retries + 1):
        logging.info(f"[Попытка {attempt}] открыть доступ к таблице '{title}'")
        
        try:
            spreadsheet = gc.open(title)
            logging.info(f"✅ Таблица '{title}' успешно открыта")
            return spreadsheet
            
        except gspread.APIError as e:
            error_code = e.response.status_code if hasattr(e, 'response') else None
            logging.info(f"⚠️ [Попытка {attempt}/{retries}] APIError {error_code}: {e}")
            
            if error_code == 503:
                if attempt < retries:
                    logging.info(f"⏳ Ожидание {delay} секунд перед повторной попыткой...")
                    time.sleep(delay)
                    # Увеличиваем задержку для следующей попытки (exponential backoff)
                    delay *= 2
                else:
                    logging.error("❌ Все попытки исчерпаны")
                    raise
            else:
                # Другие ошибки API (403, 404 и т.д.) - не повторяем
                raise
                
        except gspread.SpreadsheetNotFound:
            logging.info(f"❌ Таблица '{title}' не найдена")
            raise
            
        except Exception as e:
            logging.error(f"⚠️ [Попытка {attempt}/{retries}] Неожиданная ошибка: {e}")
            if attempt < retries:
                logging.error(f"⏳ Ожидание {delay} секунд...")
                time.sleep(delay)
                delay *= 2
            else:
                raise RuntimeError(f"Не удалось открыть таблицу '{title}' после {retries} попыток.")

async def get_funnel_v3(date_start: None, date_end: None, account: str, api_token: str):
    """Получение статистики по воронке продаж Wildberries"""
    products_list = []
    headers = {"Authorization": api_token}
    normal_delay = 2
    retry_delay = 20
    url = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
    start = date_start
    end = date_end
    limit = 1000
    offset = 0
    max_attempts = 10
    attempt = 0

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            payload = {
                "selectedPeriod": {
                    "start": start.strftime("%Y-%m-%d"),
                    "end": end.strftime("%Y-%m-%d")
                },
                "limit": limit,
                "offset": offset
            }

            try:
                async with session.post(url, json=payload) as res:
                    if res.status == 200:
                        data = await res.json()
                        products = data.get("data", {}).get("products", [])

                        if not products:
                            logging.info(f"📭 Нет данных для {account}")
                            break

                        for p in products:
                            p["account"] = account
                        products_list.extend(products)

                        logging.info(f"✅ Получено {len(products_list)} товаров ({len(products)} новых) для {account} за период {payload['selectedPeriod']}")

                        if len(products) < limit:
                            break

                        offset += len(products)
                        attempt = 0
                        await asyncio.sleep(normal_delay)

                    elif res.status == 429:
                        logging.info(f"⚠️ Ошибка 429 для {account}: слишком много запросов, ждем {retry_delay} сек.")
                        await asyncio.sleep(retry_delay)
                        attempt += 1
                        if attempt >= max_attempts:
                            logging.info(f"🚫 Превышено число попыток ({max_attempts}) для {account}")
                            break
                        continue

                    elif res.status in (400, 401, 403):
                        err = await res.json()
                        logging.info(f"⚠️ Ошибка {res.status} для {account}: {err.get('detail', 'Ошибка доступа')}")
                        return None

                    else:
                        logging.info(f"⚠️ Неожиданный статус {res.status} для {account}")
                        attempt += 1
                        if attempt >= max_attempts:
                            break

            except aiohttp.ClientError as err:
                logging.info(f"🌐 Сетевая ошибка: {err}")
                attempt += 1
                if attempt >= max_attempts:
                    break

            except Exception as e:
                logging.info(f"💥 Неожиданная ошибка: {e}")
                break

    if products_list:
        logging.info(f"🟢 Завершено получение данных по {account}. Всего товаров: {len(products_list)}")
        return products_list
    else:
        logging.info(f"❌ Не удалось получить данные по воронке продаж для {account}")
        return None

async def fetch_all(date_start: int, date_end: None):
    # Создаем задачник для получения данных о поставках по всем аккаунтам асинхронно
    tasks = [get_funnel_v3(date_start, date_end, account, api_token) for account, api_token in load_api_tokens().items()]
    res = await asyncio.gather(*tasks)
    return res


async def process_funnel_month():
    """
    Оптимизированная версия: собираем ВСЕ данные в один DataFrame за несколько месяцев
    """
    # === 1. ПОЛУЧАЕМ ДАТЫ ДЛЯ 12 МЕСЯЦЕВ до текущего ===
    current_month = datetime.now().month
    date_ranges = []
    for month_num in range(0, 2):
        year = datetime.now().year
        month = current_month - month_num
        if month <= 0:
            month += 12
            year -= 1
        first_date, last_date = get_first_and_last_day(month)
        if month == current_month:
            last_date = (datetime.now()-timedelta(days=1)).date()
        date_ranges.append((first_date, last_date))
    
    logging.info(f"📅 Запрашиваем данные за {len(date_ranges)} месяцев...")
    
    # === 2. ПАРАЛЛЕЛЬНЫЙ ЗАПРОС ВСЕХ МЕСЯЦЕВ ===
    tasks = [fetch_all(first, last) for first, last in date_ranges]
    results = await asyncio.gather(*tasks)
    
    logging.info(f"✅ Получено {sum(len(r) for r in results)} записей")
    
    # === 3. ОБЪЕДИНЯЕМ ВСЕ ДАННЫЕ В ОДИН СПИСОК ===
    all_products = []
    for result in results:
        for acc_data in result:
            if acc_data:
                all_products.extend(acc_data)
    
    logging.info(f"📦 Обработано {len(all_products)} товаров")
    
    # === 4. ОБРАБОТКА ВСЕХ ДАННЫХ ===
    rows = []
    for product in all_products:
        # Извлекаем данные 
        prod_info = product.get("product", {})
        stat = product.get("statistic", {})
        selected = stat.get("selected", {})
        time_to_ready = selected.get("timeToReady", {})
        
        # Базовая информация
        row = {
            "account": product.get("account"),
            "nm_id": prod_info.get("nmId"),
            "vendor_code": prod_info.get("vendorCode"),  
            "title": prod_info.get("title"),
            "subject_id": prod_info.get("subjectId"),
            "subject_name": prod_info.get("subjectName"),
            "brand_name": prod_info.get("brandName"),
            "product_rating": prod_info.get("productRating"),
            "feedback_rating": prod_info.get("feedbackRating"),
            "stocks_wb": prod_info.get("stocks", {}).get("wb"),
            "stocks_mp": prod_info.get("stocks", {}).get("mp"),
            "balance_sum": prod_info.get("stocks", {}).get("balanceSum"),
        }
        
        # Метрики selected
        row.update({
            "open_count": selected.get("openCount"),
            "cart_count": selected.get("cartCount"),
            "order_count": selected.get("orderCount"),
            "orders_sum": selected.get("orderSum"),
            "buyout_count": selected.get("buyoutCount"),
            "buyout_sum": selected.get("buyoutSum"),
            "cancel_count": selected.get("cancelCount"),
            "cancel_sum": selected.get("cancelSum"),
            "avg_price": selected.get("avgPrice"),
            "avg_orders_count_per_day": selected.get("avgOrdersCountPerDay"),
            "share_order_percent": selected.get("shareOrderPercent"),
            "add_to_wish_list": selected.get("addToWishlist"),
            "time_to_ready": (
                time_to_ready.get("days", 0) * 24 * 60 +
                time_to_ready.get("hours", 0) * 60 +
                time_to_ready.get("mins", 0)
            ),
            "localization_percent": selected.get("localizationPercent"),
            "date": selected.get("period", {}).get("end"),
        })
        
        rows.append(row)
            
    # === 5. ОДИН DataFrame ===
    df_full = pd.DataFrame(rows)
    
    # === 6. Создаем новые колонки ===
    df_full['month'] = pd.to_datetime(df_full['date']).dt.strftime('%m-%Y')
    df_full['wild'] = df_full['vendor_code'].str.extract(r'(wild\d+)')
    
    logging.info(f"⚡ DataFrame создан: {len(df_full)} строк за {len(date_ranges)} месяцев")
    
    return df_full

def create_insert_table_db_sync(df: pd.DataFrame, table_name: str, columns_type: dict, key_columns: tuple):
    load_dotenv()
    
    user = os.getenv('USER_2')
    password = os.getenv('PASSWORD_2')
    database = os.getenv('NAME_2') 
    host = os.getenv('HOST_2')
    port = os.getenv('PORT_2')
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = None
    
    try:
        engine = create_engine(connection_string)
        
        # Проверка типов данных
        valid_types = ['INTEGER', 'BIGINT', 'SMALLINT', 'NUMERIC', 'DATE', 'TIMESTAMP', 'BOOLEAN', 'TEXT', 'VARCHAR']
        for col, dtype in columns_type.items():
            if not dtype.strip():
                raise ValueError(f"Пустой тип данных для колонки {col}")
            base_type = dtype.split('(')[0].strip().upper()
            if base_type not in valid_types:
                raise ValueError(f"Недопустимый тип данных для колонки {col}: {dtype}")

        # Проверка и добавление отсутствующих колонок
        missing_cols = set(columns_type.keys()) - set(df.columns)
        for col in missing_cols:
            df[col] = None
            logging.info(f"Добавлена отсутствующая колонка {col} с None")
        
        extra_cols = set(df.columns) - set(columns_type.keys())
        if extra_cols:
            logging.warning(f"Лишние колонки в DataFrame: {extra_cols}, они будут проигнорированы")

        # Формирование SQL для создания таблицы
        columns_definition = ", ".join([f"{col} {dtype}" for col, dtype in columns_type.items()])
        unique_constraint = f", CONSTRAINT unique_{table_name} UNIQUE ({', '.join(key_columns)})" if key_columns else ""

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_definition}{unique_constraint}
            )
        """

        # Создание таблицы
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        
        # Подготовка данных для вставки
        columns = list(columns_type.keys())
        
        # Создание временной таблицы для UPSERT
        temp_table = f"temp_{table_name}"
        
        # Вставляем данные во временную таблицу
        df[columns].to_sql(temp_table, engine, if_exists='replace', index=False)
        
        # Выполняем UPSERT из временной таблицы с явным преобразованием типов
        with engine.connect() as conn:
            if key_columns:
                # Формируем SELECT с явным преобразованием типов
                select_columns = []
                for col in columns:
                    if columns_type[col].upper() == 'TIMESTAMP':
                        select_columns.append(f"CAST({col} AS TIMESTAMP)")  # Явное преобразование
                    else:
                        select_columns.append(col)
                
                updates = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in key_columns])
                upsert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    SELECT {', '.join(select_columns)} FROM {temp_table}
                    ON CONFLICT ({', '.join(key_columns)}) 
                    DO UPDATE SET {updates}
                """
            else:
                # Простая вставка с преобразованием типов
                select_columns = []
                for col in columns:
                    if columns_type[col].upper() == 'TIMESTAMP':
                        select_columns.append(f"CAST({col} AS TIMESTAMP)")
                    else:
                        select_columns.append(col)
                
                upsert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    SELECT {', '.join(select_columns)} FROM {temp_table}
                """
            
            conn.execute(text(upsert_query))
            # Удаляем временную таблицу
            conn.execute(text(f"DROP TABLE {temp_table}"))
            conn.commit()
        
        logging.info(f"Успешно сохранено {len(df)} строк в {table_name}")
        
    except SQLAlchemyError as e:
        logging.error(f"Ошибка при работе с БД: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Неожиданная ошибка: {str(e)}")
        raise
    finally:
        if engine:
            engine.dispose()

# === Исполняемая функция для получения данных по воронке продаж по месяцам ===
async def main_funnel():
    # === Запускаем и функцию и получаем данные ===
    df_result = await process_funnel_month()
    # Удаляем дубликаты
    df_result = df_result.drop_duplicates()
    # Приводим колонку к типу данных дата
    df_result['date'] = pd.to_datetime(df_result['date']).dt.date
    # === Добавляем данные в БД ===
    table_name = 'funnel_month'
    columns_type = {
        'account': 'VARCHAR(255)',
        'nm_id': 'BIGINT',
        'vendor_code': 'VARCHAR(255)',
        'title': 'VARCHAR(255)',
        'subject_id': 'BIGINT',
        'subject_name': 'VARCHAR(255)',
        'brand_name': 'VARCHAR(255)',
        'product_rating': 'NUMERIC(5,2)',
        'feedback_rating': 'NUMERIC(5,2)',
        'stocks_wb': 'BIGINT',
        'stocks_mp': 'BIGINT',
        'balance_sum': 'BIGINT',
        'open_count': 'BIGINT',
        'cart_count': 'BIGINT',
        'order_count': 'BIGINT',
        'orders_sum': 'BIGINT',
        'buyout_count': 'BIGINT',
        'buyout_sum': 'BIGINT',
        'cancel_count': 'BIGINT',
        'cancel_sum': 'BIGINT',
        'avg_price': 'NUMERIC(12,2)',
        'avg_orders_count_per_day': 'NUMERIC(10,2)',
        'share_order_percent': 'NUMERIC(10,2)',
        'add_to_wish_list': 'BIGINT',
        'time_to_ready': 'BIGINT',
        'localization_percent': 'NUMERIC(10,2)',
        'date': 'DATE',
        'month': 'TEXT',
        'wild': 'TEXT'
    }

    # Ключевые колонки для UPSERT
    key_columns = ('nm_id', 'date', 'account')  # как первичный ключ
    create_insert_table_db_sync(df_result, table_name, columns_type, key_columns)
    print(f"Данные добавлены в БД {table_name}")

def funnel_month_to_gs():
    """ Выгрузка данных из БД funnel_month
    в гугл-таблицу"""
    # === Работа с БД
    # Параметры подключения к БД
    name = os.getenv('NAME_2')
    user = os.getenv('USER_2')
    password = os.getenv('PASSWORD_2')
    host = os.getenv('HOST_2')
    port = os.getenv('PORT_2')
    table_name = "funnel_month"
    # Получаем нужные данные
    connection = create_connection(name, user, password, host, port)
    query_1 = f"""SELECT * FROM {table_name}
                WHERE "date" >= DATE_TRUNC('year', CURRENT_DATE)
                AND "date" <= CURRENT_DATE - INTERVAL '1 day';"""
    # Помещаем данные в датафрейм
    df = get_db_table(query_1, connection)
    # Определяем порядок расположения колонок
    df_month_cols_order = ["nm_id", "brand_name", "title", "date", "open_count", "cart_count", "order_count", "orders_sum", "buyout_count", "buyout_sum",
                    "cancel_count", "cancel_sum", "avg_price", "avg_orders_count_per_day", "add_to_wish_list", "localization_percent", "time_to_ready", 
                    "stocks_mp", "stocks_wb", "wild", "account", "month"]
    df_month = df[df_month_cols_order]
    # df_month['month'] = df_month['month'].astype(str)
    # df_month['date'] = df_month['date'].astype(str)
    # df_month = df_month.astype(str)
    table = safe_open_spreadsheet("План РК")
    sheet_profit = table.worksheet("БД Воронка месяц")

    # Полная замена листа
    sheet_profit.clear()
    # === 2. ЗАПИСЫВАЕМ DataFrame ЦЕЛИКОМ ===
    # Метод set_with_dataframe библиотеки gspread_dataframe
    set_with_dataframe(sheet_profit, df_month, resize=True)

    formatted_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    max_columns = sheet_profit.col_count
    sheet_profit.update_cell(1, max_columns, formatted_time)
    print("Данные загружены")
