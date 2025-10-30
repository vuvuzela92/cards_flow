import asyncio
import aiohttp
import pandas as pd
import logging
from datetime import datetime, timedelta
import json
import traceback
from dotenv import load_dotenv
import os
import asyncpg




# Функция для загрузки API токенов из файла tokens.json
def load_api_tokens():
    with open('tokens.json', encoding= 'utf-8') as f:
        tokens = json.load(f)
        return tokens
# next_request_time будет хранить время, когда можно сделать следующий запрос для каждого аккаунта
# Это нужно для соблюдения рейт-лимита Wildberries API
next_request_time = {}
# request_lock используется для синхронизации доступа к next_request_time
# Это нужно, чтобы избежать гонок при обновлении времени следующего запроса
# Если несколько корутин одновременно попытаются обновить время, это может привести к ошибкам
request_lock = asyncio.Lock()
# semaphore ограничивает количество одновременных запросов к API
# semaphore = asyncio.Semaphore(8)  # максимум 8 одновременных запросов

async def get_fin_reports_async(account: str, api_token: str, date_from: str, date_to: str, request_lock: asyncio.Lock, next_request_time: dict):
    """Получаем финансовые отчёты с правильной пагинацией и retry."""
    # Глобальный словарь: когда можно следующий запрос по аккаунту

    async with asyncio.Semaphore(8) as semaphore:
        print(f"🔍 {account} | Запрос за {date_from} – {date_to}")

    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
    headers = {"Authorization": api_token}
    #  указатель на последнюю обработанную строку, используется для пагинации.
    rrdid = 0
    # список, в который будут складываться все DataFrame с данными.
    all_data = []

    timeout = aiohttp.ClientTimeout(total=120, connect=30, sock_read=60, sock_connect=15)
    # Создаётся асинхронная HTTP-сессия. Все запросы будут идти через неё.
    async with aiohttp.ClientSession(timeout=timeout) as session:
        attempt = 0
        max_attempts = 20
        last_rrdid = None # защита от зацикливания (если rrdid не меняется)

        while attempt < max_attempts:
            try:
                # Проверяем, когда можно сделать следующий запрос для этого аккаунта
                async with request_lock:
                    now = datetime.now()
                    # Если для аккаунта нет времени следующего запроса, устанавливаем его в текущее время
                    last_call = next_request_time.get(account, now)
                    # Если сейчас меньше времени следующего запроса, ждём
                    if now < last_call:
                        # Вычисляем, сколько нужно подождать
                        wait = (last_call - now).total_seconds()
                        logging.warning(f"[{account}] Ждём {wait:.1f} сек до следующего запроса")
                        await asyncio.sleep(wait)
                params = {
                    "dateFrom": date_from,
                    "dateTo": date_to,
                    "limit": 50000,
                    "rrdid": rrdid,
                }
                # Отправляем GET-запрос к API Wildberries
                async with session.get(url, headers=headers, params=params) as response:
                    # ✅ Если получаем 429 ошибку, значит превысили лимит запросов.
                    if response.status == 429:
                        logging.warning(f"[429] {account} | Лимит. Ждём 65 сек...")
                        async with request_lock:
                            next_request_time[account] = datetime.now() + timedelta(seconds=65)
                        attempt += 1
                        await asyncio.sleep(1)
                        continue

                    # ✅ Если выходит 400 ошибка, проверяем не связана ли она с критическими ошибками.
                    # если нет, то отправляем повторный запрос
                    if response.status == 400:
                        text = await response.text()
                        logging.warning(f"[400] {account} | Bad Request: {text[:500]}")

                        # Если ошибка в токене или rrdid — нельзя повторять
                        critical = ["invalid", "token", "rrdid", "malformed", "parameter"]
                        if any(word in text.lower() for word in critical):
                            logging.error(f"🔴 Критическая ошибка 400: {text[:200]}. Пропускаем.")
                            break

                        # Если не критично — пробуем повторить
                        logging.info(f"[400] Повторяем запрос через задержку...")
                        async with request_lock:
                            next_request_time[account] = datetime.now() + timedelta(seconds=65)
                        attempt += 1
                        await asyncio.sleep(2 * attempt)
                        continue

                    # Остальные статусы
                    if response.status != 200:
                        text = await response.text()
                        logging.error(f"[{response.status}] {account}: {text[:500]}")
                        break

                    raw_text = await response.text()
                    if not raw_text.strip():
                        logging.warning(f"📡 {account}: Пустой ответ")
                        break

                    try:
                        data = json.loads(raw_text)
                        if isinstance(data, list) and not data:
                            break
                        print(f"Получены сырые данные ({len(data)} строк):", data[:1])  # Первая строка для примера
                    except json.JSONDecodeError as je:
                        logging.error(f"💥 JSON Error {account}: {je}")
                        logging.error(f"Raw (first 1000): {raw_text[:1000]}")
                        break

                    if not data or (isinstance(data, dict) and data.get("errors")):
                        logging.info(f"🟢 {account}: Нет данных за {date_from}")
                        break

                    df = pd.DataFrame(data)
                    if df.empty:
                        logging.info(f"🟡 {account}: Пустой DataFrame")
                        break
                    print("Создан DataFrame с колонками:", df.columns.tolist())
                    print("Пример данных:", df.iloc[0].to_dict())

                    df["account"] = account
                    df['dlv_prc'] = df['dlv_prc'].astype(str)
                    all_data.append(df)

                    logging.info(f"📥 {account} | +{len(df)} строк | {date_from}–{date_to}")
                    print(f'По {account} получаем rrdid->{df["rrd_id"].iloc[-1]}')

                    next_rrdid = df["rrd_id"].iloc[-1]
                    if next_rrdid == rrdid or next_rrdid == last_rrdid:
                        break
                    if next_rrdid <= rrdid:
                        break
                    rrdid = next_rrdid
                    last_rrdid = next_rrdid

                    delay = 60
                    if len(df) >= 25000:
                        delay = 70

                    async with request_lock:
                        next_request_time[account] = datetime.now() + timedelta(seconds=delay)

            except aiohttp.ClientPayloadError as e:
                logging.warning(f"📡 [Payload] {account}: {e}. Попытка {attempt + 1}")
                attempt += 1
                await asyncio.sleep(5 * attempt)
                continue

            except asyncio.TimeoutError:
                logging.warning(f"⏰ Timeout {account}, попытка {attempt + 1}")
                attempt += 1
                await asyncio.sleep(5 * attempt)
                continue

            except (aiohttp.ClientConnectorError, ConnectionResetError) as e:
                logging.warning(f"🔌 Сеть {account}: {type(e).__name__}, попытка {attempt + 1}")
                attempt += 1
                await asyncio.sleep(5 * attempt)
                continue

            except Exception as e:
                logging.error(f"❌ Ошибка {account}: {type(e).__name__}: {e}")
                logging.error(f"TRACEBACK:\n{traceback.format_exc()}")
                break

        result = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
        # Преобразование дат
        date_cols_list = ['date_from', 'date_to', 'create_dt', 'fix_tariff_date_from',
                        'fix_tariff_date_to', 'rr_dt', 'order_dt', 'sale_dt'] 

        for col in date_cols_list:
            if col in result.columns:
                result[col] = pd.to_datetime(result[col], errors='coerce')
                if pd.api.types.is_datetime64tz_dtype(result[col]):
                    result[col] = result[col].dt.tz_localize(None)
                if col in ['order_dt', 'sale_dt']:
                    result[col] = pd.Series(
                        [x.to_pydatetime() if pd.notna(x) else None for x in result[col]],
                        index=result.index,
                        dtype='object'
                    )
                else:
                    result[col] = result[col].apply(
                        lambda x: x.date() if pd.notna(x) else None
                    )

        # Преобразование числовых типов
        for col in result.columns:
            if result[col].dtype in ['int64', 'int32', 'int16', 'int8']:
                result[col] = pd.Series(
                    [None if pd.isna(x) else int(x) for x in result[col]],
                    index=result.index,
                    dtype='object'
                )
            elif result[col].dtype in ['float64', 'float32', 'float16']:
                result[col] = pd.Series(
                    [None if pd.isna(x) else float(x) for x in result[col]],
                    index=result.index,
                    dtype='object'
                )
            elif result[col].dtype == 'bool':
                result[col] = pd.Series(
                    [None if pd.isna(x) else bool(x) for x in result[col]],
                    index=result.index,
                    dtype='object'
                )

        # Явная конвертация BOOLEAN-колонок (для надёжности, из предыдущей ошибки)
        bool_cols = ['is_kgvp_v2', 'srv_dbs', 'is_legal_entity']
        for col in bool_cols:
            if col in result.columns:
                result[col] = pd.Series(
                    [None if pd.isna(x) else bool(x) for x in result[col]],
                    index=result.index,
                    dtype='object'
                )

        # Преобразование текстовых колонок (VARCHAR/TEXT)
        text_cols = [
            'currency_name', 'suppliercontract_code', 'dlv_prc', 'subject_name', 'brand_name',
            'sa_name', 'ts_name', 'barcode', 'doc_type_name', 'office_name', 'supplier_oper_name',
            'gi_box_type_name', 'payment_processing', 'acquiring_bank', 'ppvz_office_name',
            'ppvz_supplier_name', 'ppvz_inn', 'declaration_number', 'bonus_type_name',
            'sticker_id', 'site_country', 'rebill_logistic_org', 'kiz', 'trbx_id', 'account'
        ]
        for col in text_cols:
            if col in result.columns:
                result[col] = pd.Series(
                    [None if pd.isna(x) else str(x) for x in result[col]],
                    index=result.index,
                    dtype='object'
                )

        logging.info(f"✅ {account} | Загружено {len(result)} строк за {date_from}–{date_to}")
        print(result.info())
        print(result.dtypes)
        return result

# Глобальный лок для создания таблицы
table_creation_lock = asyncio.Lock()

async def create_insert_table_db_async(df: pd.DataFrame, table_name: str, columns_type: dict, key_columns: tuple):
    load_dotenv()

    conn = None
    try:
        conn = await asyncpg.connect(
            user=os.getenv('USER_2'),
            password=os.getenv('PASSWORD_2'),
            database=os.getenv('NAME_2'),
            host=os.getenv('HOST_2'),
            port=os.getenv('PORT_2'),
            timeout=10
        )

        # Проверка типов данных
        valid_types = ['INTEGER', 'BIGINT', 'SMALLINT', 'NUMERIC', 'DATE', 'TIMESTAMP', 'BOOLEAN', 'TEXT', 'VARCHAR']
        for col, dtype in columns_type.items():
            if not dtype.strip():
                raise ValueError(f"Пустой тип данных для колонки {col}")
            base_type = dtype.split('(')[0].strip().upper()
            if base_type not in valid_types:
                raise ValueError(f"Недопустимый тип данных для колонки {col}: {dtype}")
            if '(' in dtype and base_type not in ['NUMERIC', 'VARCHAR']:
                raise ValueError(f"Скобки недопустимы для типа {base_type} в колонке {col}: {dtype}")

        # Проверка и добавление отсутствующих колонок
        missing_cols = set(columns_type.keys()) - set(df.columns)
        for col in missing_cols:
            df[col] = None
            logging.info(f"Добавлена отсутствующая колонка {col} с None")
        extra_cols = set(df.columns) - set(columns_type.keys())
        if extra_cols:
            logging.warning(f"Лишние колонки в DataFrame: {extra_cols}, они будут проигнорированы")

        # Формирование SQL для колонок
        columns_definition = ", ".join([f"{col} {dtype}" for col, dtype in columns_type.items()])
        # создает SQL-выражение для уникального ограничения (UNIQUE constraint) в таблице базы данных.
        unique_constraint = f"CONSTRAINT unique_{table_name} UNIQUE ({', '.join(key_columns)})" if key_columns else ""

        # Отладка: вывод SQL-запроса
        create_table_query = f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = '{table_name}') THEN
                    CREATE TABLE {table_name} (
                        {columns_definition}{', ' + unique_constraint if unique_constraint else ''}
                    );
                END IF;
            END$$;
        """



        # Безопасное создание таблицы
        async with table_creation_lock:
            await conn.execute(create_table_query)
        
        # Подготовка данных для вставки
        columns = list(columns_type.keys())  # Используем только колонки из columns_type
        records = [tuple(None if pd.isna(x) else x for x in row) for row in df[columns].to_records(index=False)]

        # Формирование UPSERT-запроса
        updates = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in key_columns])
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join([f'${i+1}' for i in range(len(columns))])})
            ON CONFLICT ON CONSTRAINT unique_{table_name}
            DO UPDATE SET {updates}
        """
        
        await conn.executemany(query, records)
        logging.info(f"Успешно сохранено {len(df)} строк в {table_name}")
        
    except Exception as e:
        logging.error(f"Ошибка при работе с БД: {str(e)}")
        raise
    finally:
        if conn:
            await conn.close()



async def fetch_all_data(accounts_tokens, num_weeks=1):
    today = datetime.today()
    weekday = today.weekday()
    current_sunday = today - timedelta(days=(weekday + 1) % 7)
    processed_weeks = 0

    for week in range(num_weeks):
        current_monday = current_sunday - timedelta(days=6)
        date_from = current_monday.strftime('%Y-%m-%d')
        date_to = current_sunday.strftime('%Y-%m-%d')

        logging.info(f"📅 Загружаем неделю {week + 1}/{num_weeks}: {date_from} – {date_to}")

        tasks = [
            get_fin_reports_async(account, token, date_from, date_to, request_lock, next_request_time)
            for account, token in accounts_tokens.items()
        ]

        weekly_results = await asyncio.gather(*tasks, return_exceptions=True)

        table_name = 'fin_reports_full'
        columns_type = {
            "realizationreport_id": "INTEGER",
            "date_from": "DATE",
            "date_to": "DATE",
            "create_dt": "DATE",
            "currency_name": "TEXT",
            "suppliercontract_code": "TEXT",
            "rrd_id": "BIGINT",
            "gi_id": "BIGINT",
            "dlv_prc": "TEXT",
            "fix_tariff_date_from": "DATE",
            "fix_tariff_date_to": "DATE",
            "subject_name": "TEXT",
            "nm_id": "BIGINT",
            "brand_name": "TEXT",
            "sa_name": "TEXT",
            "ts_name": "TEXT",
            "barcode": "TEXT",
            "doc_type_name": "TEXT",
            "quantity": "INTEGER",
            "retail_price": "NUMERIC(12,2)",
            "retail_amount": "NUMERIC(12,2)",
            "sale_percent": "SMALLINT",
            "commission_percent": "NUMERIC(5,2)",
            "office_name": "TEXT",
            "supplier_oper_name": "TEXT",
            "order_dt": "TIMESTAMP",
            "sale_dt": "TIMESTAMP",
            "rr_dt": "DATE",
            "shk_id": "BIGINT",
            "retail_price_withdisc_rub": "NUMERIC(12,2)",
            "delivery_amount": "INTEGER",
            "return_amount": "INTEGER",
            "delivery_rub": "NUMERIC(12,2)",
            "gi_box_type_name": "TEXT",
            "product_discount_for_report": "NUMERIC(5,2)",
            "supplier_promo": "NUMERIC(12,2)",
            "ppvz_spp_prc": "NUMERIC(5,2)",
            "ppvz_kvw_prc_base": "NUMERIC(5,2)",
            "ppvz_kvw_prc": "NUMERIC(5,2)",
            "sup_rating_prc_up": "NUMERIC(5,2)",
            "is_kgvp_v2": "BOOLEAN",
            "ppvz_sales_commission": "NUMERIC(12,2)",
            "ppvz_for_pay": "NUMERIC(12,2)",
            "ppvz_reward": "NUMERIC(12,2)",
            "acquiring_fee": "NUMERIC(12,2)",
            "acquiring_percent": "NUMERIC(5,2)",
            "payment_processing": "TEXT",
            "acquiring_bank": "TEXT",
            "ppvz_vw": "NUMERIC(12,2)",
            "ppvz_vw_nds": "NUMERIC(12,2)",
            "ppvz_office_name": "TEXT",
            "ppvz_office_id": "INTEGER",
            "ppvz_supplier_id": "INTEGER",
            "ppvz_supplier_name": "TEXT",
            "ppvz_inn": "TEXT",
            "declaration_number": "TEXT",
            "bonus_type_name": "TEXT",
            "sticker_id": "TEXT",
            "site_country": "TEXT",
            "srv_dbs": "BOOLEAN",
            "penalty": "NUMERIC(12,2)",
            "additional_payment": "NUMERIC(12,2)",
            "rebill_logistic_cost": "NUMERIC(12,2)",
            "rebill_logistic_org": "TEXT",
            "storage_fee": "NUMERIC(12,2)",
            "deduction": "NUMERIC(12,2)",
            "acceptance": "NUMERIC(12,2)",
            "assembly_id": "BIGINT",
            "srid": "TEXT",
            "report_type": "SMALLINT",
            "is_legal_entity": "BOOLEAN",
            "trbx_id": "TEXT",
            "installment_cofinancing_amount": "NUMERIC(12,2)",
            "wibes_wb_discount_percent": "SMALLINT",
            "cashback_amount": "NUMERIC(12,2)",
            "cashback_discount": "NUMERIC(5,2)",
            "account": "VARCHAR(50)",
            }
        key_columns = ['realizationreport_id', 'rrd_id', 'srid']

        save_tasks = []
        for result in weekly_results:
            if isinstance(result, Exception):
                logging.error(f"Ошибка при загрузке недели {week + 1}: {result}")
                continue
            if isinstance(result, pd.DataFrame) and not result.empty:
                save_tasks.append(
                    create_insert_table_db_async(result, table_name, columns_type, key_columns)
                )

        if save_tasks:
            await asyncio.gather(*save_tasks, return_exceptions=True)
        processed_weeks += 1
        logging.info(f"✅ Завершена обработка недели {week + 1}/{num_weeks}")

        current_sunday -= timedelta(days=7)

    logging.info(f"✅ Все данные сохранены в БД. Обработано {processed_weeks}/{num_weeks} недель")