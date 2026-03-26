import pandas as pd
from utils_sql import create_connection, get_db_table
from datetime import datetime, timedelta
from utils_gs import safe_open_spreadsheet
from gspread_dataframe import set_with_dataframe


# Создание подключения к базе данных
connection = create_connection()

query = f"""
SELECT * FROM conditions_calculation cc
WHERE cc.date >= '2025-12-01'
ORDER BY cc.date ASC,
	cc.account;
"""

# Загрузка данных в датафрейм
df = get_db_table(query, connection)  

# Соединение закрыто
connection.close()

# Доступ к Google Sheets
table_name = safe_open_spreadsheet('Условный расчет')
sheet = table_name.worksheet('Справочная информация')
set_with_dataframe(sheet, df)