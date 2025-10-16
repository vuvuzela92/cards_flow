import requests
import time
from my_utils import load_api_tokens
import gspread


def cards_list_wb(api_token):
  """Получение информации о карточке товара"""
  url = 'https://content-api.wildberries.ru/content/v2/get/cards/list'
  headers = {
      "Authorization" : api_token
  }
  nmID = None
  updatedAt = None
  counter = 0
  total = 100
  limit = 100
  all_cards_data = []

  while total >= limit:
    print(f'Получены данные по {counter} карточкам')
    payload = {
      "settings": {
        "sort": {
          "ascending": False
        },
        "filter": {
          "withPhoto": -1
        },
        "cursor": {
          "updatedAt": updatedAt,
          "nmID": nmID,
          "limit": limit
        }
      }
    }
    try:
      res = requests.post(url, headers=headers, json=payload)
      print(res.status_code)
      data = res.json()
      if res.status_code == 200:
        nmID = data['cursor']['nmID']
        updatedAt = data['cursor']['updatedAt']
        total = data['cursor']['total']
        all_cards_data.append(data)
        counter+=total
    except Exception as e:
      print(e)
    time.sleep(1)
  return all_cards_data


def get_card_info():
    """Функция собирает информацию обо всех карточках товаров на ВБ
    и возвращает данные в виде списка"""
    # Собираем информацию по всем карточкам
    cards_info_list = [] 
    for account, api_token in load_api_tokens().items():
        print(f'Получаем данные по ЛК: {account}')
        res = cards_list_wb(api_token)
        for i in range(len(res)):
            cards = res[i]['cards']
            for card in cards:
                card['account'] = account
                cards_info_list.append(card)
    return cards_info_list


def extract_characteristic(characteristic: str):
    """ Извлекаем характеристику из данных по карточке товара.
    characteristic: название нужной характеристики.
    Функция возвращает значение список словарей с """
    # список для хранения данных об НДС
    # characteristic_data_list = []
    characteristic_data_dict = {}
    cards_info = get_card_info()
    for i in range(len(cards_info)):
        # Извлекаем артикул товара
        nm_id = cards_info[i]['nmID']
        try:
            # Получаем список характеристик
            characteristics = cards_info[i]['characteristics']
            # Ищем нужную характеристику по имени
            characteristic_full = next((item for item in characteristics if item['name'] == characteristic), None)
        except:
            print(f'Нет данных по {characteristic} по {nm_id}')
            # Ищем нужную характеристику по имени
            characteristic_full = next((item for item in characteristics if item['name'] == characteristic), None)
            # Обрабатываем случаи, когда нет информации об НДС
        try:
            if characteristic_full is None:
                print(f"Не найдена характеристика {characteristic} для nm_id={nm_id}")
                # Можно записать дефолтное значение или пропустить
                char = 0  # например, стандартное значение
            else:
                # Проверяем, что 'value' существует и не пуст
                value_list = characteristic_full.get('value')
                if not value_list or len(value_list) == 0:
                    print(f"Пустое значение 'value' у nm_id={nm_id}")
                    char = ''  # дефолт
                else:
                    char = int(value_list[0])

            # Создаём словарь
            # char_dict = {nm_id: char}
            characteristic_data_dict[nm_id] = char
            # characteristic_data_list.append(char_dict)

        except (TypeError, ValueError, KeyError) as e:
            print(f"Ошибка при обработке nm_id={nm_id}: {e}")
            # Если будет исключение, то запишем 0
            # char_dict = {nm_id: 0}  # fallback
            characteristic_data_dict.get(nm_id, 0)
            # characteristic_data_list.append(char_dict)
    # return characteristic_data_list
    return characteristic_data_dict

def get_nds_target_range(headers_loc: int, col_header: str, col_art_header: str, sheet: gspread.worksheet):
    """Функция определяет диапазон для вставки данных в гугл таблицу.
       -row_loc: номер строки, на которой располагаются заголовки
       -col_header: название заголовка колонки
       -col_art_header: название колонки с артикулом. 
       -sheet: вкладка гугл тыблицы"""
    # Читаем заголовки
    header_row = sheet.row_values(headers_loc)
    # Получаю индекс колонки с артикулом
    try:
        art_col_idx = header_row.index(f'{col_art_header}') + 1  # +1 потому что индексы в Python с 0, а колонки в Google Sheets с 1
    except ValueError:
        raise ValueError(f"Колонка {col_art_header} не найдена в первой строке")
    # Получаю индекс колонки с нужным назвнием
    try:
        nds_col_idx = header_row.index(f'{col_header}') + 1
    except ValueError:
        raise ValueError(f"Колонка {col_header} не найдена в первой строке")
    # Считываем данные об артикулах из гугл таблицы в том же порядке
    art_values = sheet.col_values(art_col_idx)[headers_loc:]  # артикулы без заголовка
    # Определим последнюю строку
    last_row = headers_loc + len(art_values)  # 1 (заголовок) + количество данных
    # Определяем диапазон для записи в искомую колонку 
    col_header_range = f"{gspread.utils.rowcol_to_a1(headers_loc+1, nds_col_idx)}:{gspread.utils.rowcol_to_a1(last_row, nds_col_idx)}"
    return col_header_range

def prepare_insert_nds_to_unit(headers_loc: int, col_art_header: str, nds_dict_data: dict, sheet: gspread.worksheet):
    """ Подготавливаем данные для вставки в гугл таблицу.
       -row_loc: номер строки, на которой располагаются заголовки
       -col_header: название заголовка колонки
       -col_art_header: название колонки с артикулом. 
       -sheet: вкладка гугл тыблицы"""
    # Подготовим список значений для записи
    nds_values_list = []
    # Читаем заголовки
    header_row = sheet.row_values(headers_loc)
    # Получаю индекс колонки с нужным назвнием
    try:
        art_col_idx = header_row.index(f'{col_art_header}') + 1  # +1 потому что индексы в Python с 0, а колонки в Google Sheets с 1
    except ValueError:
        raise ValueError(f"Колонка {col_art_header} не найдена в первой строке")
    # Считываем данные об артикулах из гугл таблицы в том же порядке
    art_values = sheet.col_values(art_col_idx)[headers_loc:]  # артикулы без заголовка
    # Подготавливаем список nds_values в порядке артикулов соответствующем гугл таблице
    for art in art_values:
        try:
            # Пробуем преобразовать артикул в число
            nm_id = int(art)
        except (TypeError, ValueError):
            nds_values_list.append([""])  # не число — пусто
            continue

        # Ищем в словаре
        nds_value = nds_dict_data.get(nm_id, "")  # если нет — пусто
        nds_values_list.append([nds_value])  # важно: каждый элемент — список из одного значения
    return nds_values_list