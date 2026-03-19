import os
import json
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
from datetime import datetime
import itertools
import requests

load_dotenv()


# === НАСТРОЙКА ЛОГИРОВАНИЯ ===
logger = logging.getLogger("funnel_logger")
logger.setLevel(logging.INFO)

# Форматтер
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(funcName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 1. Хендлер в ФАЙЛ
file_handler = logging.FileHandler('funnel.log', encoding='utf-8')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# 2. Хендлер в КОНСОЛЬ
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

# Добавляем оба хендлера, если их еще нет
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def load_api_tokens():
    # Проверяем наличие файла tokens.json во всех директориях проекта
    current_dir = os.path.dirname(os.path.abspath(__file__))

    while True:
        tokens_path = os.path.join(current_dir, 'tokens.json')
        if os.path.isfile(tokens_path):
            try:
                with open(tokens_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print(f"Ошибка декодирования JSON в файле: {tokens_path}")
                return None

        # Поднимаемся на уровень выше
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            # Достигли корня диска
            break
        current_dir = parent_dir

def batchify(data, batch_size):
    """
    Splits data into batches of a specified size.

    Parameters:
    - data: The list of items to be batched.
    - batch_size: The size of each batch.

    Returns:
    - A generator yielding batches of data.
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]    
    
semaphore = asyncio.Semaphore(10)
async def adv_stat_async(campaign_ids: list, date_from: str, date_to: str, api_token: str, account: str):
    """
    Получение статистики по списку ID кампаний за указанный период.

    :param campaign_ids: список ID кампаний
    :param date_from: дата начала периода в формате YYYY-MM-DD
    :param date_to: дата окончания периода в формате YYYY-MM-DD
    :param api_token: токен для API WB
    :param account: название аккаунта
    """
    url = "https://advert-api.wildberries.ru/adv/v3/fullstats"
    headers = {"Authorization": api_token}
    batches = list(batchify(campaign_ids, 100))
    data = []
    async with semaphore:
        async with aiohttp.ClientSession(headers=headers) as session:
            for batch in batches:
                ids_str = ",".join(str(c) for c in batch)
                params = {"ids": ids_str, "beginDate": date_from, "endDate": date_to}

                print(f"Запрос для {account}: {params}")

                retry_count = 0
                while retry_count < 5:
                    try:
                        async with session.get(url, params=params) as response:
                            print(f"HTTP статус: {response.status}")

                            if response.status == 400:
                                err = await response.json()
                                print(f"Ошибка 400 {account}: {err.get('message') or err}")
                                # retry_count += 1
                                # await asyncio.sleep(60)
                                continue

                            if response.status == 429:
                                print("429 Too Many Requests — ждём минуту")
                                retry_count += 1
                                await asyncio.sleep(60)
                                continue

                            response.raise_for_status()
                            batch_data = await response.json()

                            # добавляем поле account в каждый элемент
                            for item in batch_data or []:
                                item["account"] = account
                                item["date"] = date_from
                            data.extend(batch_data or [])
                            break

                    except aiohttp.ClientError as e:
                        print(f"Сетевая ошибка для {account}: {e}")
                        retry_count += 1
                        await asyncio.sleep(30)

                # WB ограничивает 1 запрос/мин → ждём после каждого батча
                await asyncio.sleep(60)

        return data
    

def camp_list(api_token: str, account: str):
    url = 'https://advert-api.wildberries.ru/adv/v1/promotion/adverts'
    camps = []
    campaign_statuses = [9, 11]
    headers = {'Authorization': api_token}
    for status_id in campaign_statuses:
        params = {
        'status': status_id,
        'order': 'id'
                }
        payload = []
        try:
            res = requests.post(url, headers=headers, params=params, json=payload)
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            print(e)
            data = []

        if data:
                # Добавляем информацию о кабинете в данные
                for item in data:
                    item['account'] = account
                camps.append(data)
    return camps


def camp_list_manual(api_token: str, account: str):
    url = 'https://advert-api.wildberries.ru/adv/v0/auction/adverts'
    camps = []
    campaign_statuses = [9, 11]
    headers = {'Authorization': api_token}
    for status_id in campaign_statuses:
        params = {
        'status': status_id
                }
        try:
            res = requests.get(url, headers=headers, params=params)
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            print(e)
            data = []

        if data:
                # Добавляем информацию о кабинете в данные
                for item in data['adverts']:
                    item['account'] = account
                camps.append(data['adverts'])
    return camps    

async def get_all_adv_data(days_count=1):
    """ Получаем по ручной и единой РК"""
    all_adv_data = []
    tasks = []
    for account, api_token in load_api_tokens().items():
        # Получаем информацию об РК с единой ставкой
        camps_list = camp_list(api_token, account)
        # Преобразуем список списков в один список
        campaigns = list(itertools.chain(*camps_list))
        campaign_ids = [c['advertId'] for c in campaigns]
        # Получаем информацию об РК с ручной ставкой
        camps_list_2 = camp_list_manual(api_token, account)
        # Преобразуем список списков в один список
        campaigns_2 = list(itertools.chain(*camps_list_2))
        campaign_ids_2 = [c['id'] for c in campaigns_2 if c['status'] in (9, 11)]
        # Объединяем списки РК в единый
        campaign_ids.extend(campaign_ids_2)
        # Убираем дубликаты
        campaign_ids = list(set(campaign_ids))
        for day in range(1, days_count+1):
            yesterday = datetime.now() - timedelta(days=day)
            date_from = date_to = yesterday.strftime("%Y-%m-%d")
            print(f"Получаем данные за {date_from} по ЛК {account}")
            # date_range = [date_from]
            tasks.append(adv_stat_async(campaign_ids, date_from, date_to, api_token, account))
    # Получаем статистику по кампаниям
    stats = await asyncio.gather(*tasks)
    for stat in stats:
        all_adv_data.extend(stat)
    return all_adv_data
