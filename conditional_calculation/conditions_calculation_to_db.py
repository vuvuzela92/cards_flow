# %%
from utils_sql import create_connection, get_db_table, create_insert_table_db_sync
import pandas as pd
from datetime import datetime


# Создание подключения к базе данных
connection = create_connection()
# Цикл по количеству дней для расчета
days_ago = 30  # Количество дней для расчета
days_to = 1

query = f"""
    SELECT
        o.account,
        SUM(o.orders_sum_rub) AS orders_sum,
        ROUND(SUM(o.sales_sum)) AS sales_sum,
        ROUND(SUM(o.profit_by_cond_orders)) AS profit_by_ind_cond_orders,
        ROUND(SUM(o.profit_by_cond_sales)) AS profit_by_ind_cond_sales,
        SUM(o.sales_count) AS sales_count,
        SUM(o.orders_count) AS order_count,
        SUM(o.adv_spend) AS adv_spend,
        SUM(o.bonuses) AS bonuses,
        ROUND(SUM(o.profit_by_cond_sales) - SUM(o.adv_spend)) AS profit_cond_sales_minus_adv_spend,
        ROUND(SUM(o.orders_count) * AVG(o.purchase_price)) AS cost_price_orders,
        ROUND(SUM(o.sales_count) * AVG(o.purchase_price)) AS cost_price_sales,
        SUM(o.profit_by_orders) AS general_profit_orders,
        o.date
        FROM orders_articles_analyze o
    WHERE o.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days'
        AND CURRENT_DATE - INTERVAL '{days_to} days'
        AND o.account != '0'
    GROUP BY o.account,
            o.date;
        """
# Получение данных за один день и добавление в список
df = get_db_table(query, connection)
# Закрытие подключения к базе данных
connection.close()

table_name = 'conditions_calculation'
columns_type = {
    'account': 'VARCHAR(255)',
    'orders_sum': 'BIGINT',
    'sales_sum': 'BIGINT',
    'profit_by_ind_cond_orders': 'BIGINT',
    'profit_by_ind_cond_sales': 'BIGINT',
    'sales_count': 'BIGINT',
    'order_count': 'BIGINT',
    'adv_spend': 'BIGINT',
    'bonuses': 'BIGINT',
    'profit_cond_sales_minus_adv_spend': 'BIGINT',
    'cost_price_orders': 'BIGINT',
    'cost_price_sales': 'BIGINT',
    'general_profit_orders': 'BIGINT',
    'date': 'DATE'
}
# Ключевые колонки для UPSERT
key_columns = ('account', 'date')  # как первичный ключ
create_insert_table_db_sync(df, table_name, columns_type, key_columns)
print(f"Данные добавлены в БД {table_name}")




