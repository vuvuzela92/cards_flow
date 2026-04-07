from utils_sql import create_connection, get_db_table
from utils_gs import safe_open_spreadsheet
from gspread_dataframe import set_with_dataframe
from datetime import datetime

def main():
	# Устанавливаем соединение с базой данных
	connection = create_connection()
	days_count = 1  # Количество дней для расчета
	# Создаем SQL-запрос для получения данных
	query = f""" WITH fin_rep_to_pay AS(SELECT
                f.date_from,
                f.account,
                SUM(CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay ELSE 0 END) -
                SUM(CASE WHEN doc_type_name = 'Возврат' THEN ppvz_for_pay ELSE 0 END) - SUM(f.delivery_rub ) - SUM(f.penalty) - SUM(f.deduction) AS "Итого к оплате",
                SUM(CASE WHEN f.bonus_type_name ILIKE '%заём%' 
                	OR f.bonus_type_name ILIKE '%займ%'
                	OR f.bonus_type_name ILIKE '%кредит%'
                	THEN f.deduction ELSE 0 END) AS credit_transfers
        FROM daily_fin_reports_full f
        WHERE f.date_from >= '2026-01-01'
        GROUP BY f.date_from, f.account)
        SELECT f.date_from,
                SUM(f."Итого к оплате") AS "Итого к оплате",
                SUM(f.credit_transfers) AS "Кредит",
                f.account
        FROM fin_rep_to_pay f
        GROUP BY f.date_from, account;"""
	# Получаем данные из базы данных в виде DataFrame
	df = get_db_table(query, connection)
	df['updatet_at'] = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
	# # Преобразуем столбец date_from в строковый тип
	# df['date_from'] = df['date_from'].astype(str)
	# Открываем таблицу Google Sheets и выбираем нужный лист   
	table = safe_open_spreadsheet("Условный расчет")
	# Выбираем лист "ВБ_к_оплате"
	sheet = table.worksheet("ВБ_к_оплате")
	set_with_dataframe(sheet, df)
	print(f"Датафрейм загружен в гугл таблицу")

if __name__ == "__main__":
	main()