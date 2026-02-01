# %%
import pandas as pd
from utils_sql import create_connection, get_db_table
from datetime import datetime, timedelta
from utils_gs import safe_open_spreadsheet, send_df_to_google

# %%
# Список для хранения данных по каждому дню
data = []
# Создание подключения к базе данных
connection = create_connection()
# Цикл по количеству дней для расчета
days_count = 1  # Количество дней для расчета
for day in range(days_count):
    date = (datetime.now() - pd.Timedelta(days=day+1)).strftime('%Y-%m-%d')
    print(f"Выполняется запрос за дату: {date}")
    query = f"""
    SELECT * FROM conditions_calculation
    WHERE date = '{date}';
    """
    df_one_day = get_db_table(query, connection)  
    data.append(df_one_day)

connection.close()
# Объединение всех данных в один датафрейм
df = pd.concat(data, ignore_index=True)
# Доступ к Google Sheets
table_name = safe_open_spreadsheet('Условный расчет')
sheet = table_name.worksheet('Справочная информация')
# Преобразование всех столбцов в строковый тип
df = df.astype(str)
# Отправка данных в Google Sheets
send_df_to_google(df, sheet)