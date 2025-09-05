import sys
import os
# Добавляем в sys.path директорию проекта (где лежат my_card_utils.py и my_utils.py)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Теперь можно импортировать
from my_card_utils import extract_characteristic, get_nds_target_range, prepare_insert_nds_to_unit
from my_utils import safe_open_spreadsheet


if __name__ == "__main__":
    # Определяем нужную для извлечения характеристику
    characteristic = 'Ставка НДС'
    # Извлекаем нужные данные
    characteristic_data = extract_characteristic(characteristic)
    # Превращаем список словарей в один словарь: {nm_id: характеристика}
    nds_dict_data = {
        nm_id: nds for d in characteristic_data for nm_id, nds in d.items()}

    # Открываем гугл таблицу
    table = safe_open_spreadsheet('UNIT 2.0 (tested)')
    # Определяем лист для вставки данных
    sheet = table.worksheet('MAIN (tested)')

    # Строка в которой расположены заголовки
    headers_loc = 1
    # Получаем название искомой колонки
    col_header = 'НДС'
    # Получаем название колонки с артикулом
    col_art_header = 'Артикул'
    # Получаем диапазон для вставки
    nds_range = get_nds_target_range(
        headers_loc, col_header, col_art_header, sheet)

    # Получаем значения НДС для вставки данных
    nds_values = prepare_insert_nds_to_unit(
        headers_loc, col_art_header, nds_dict_data, sheet)
    # Записываем список списков в диапазон
    sheet.update(range_name=nds_range, values=nds_values)
