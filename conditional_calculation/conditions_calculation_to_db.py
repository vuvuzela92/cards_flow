# %%
from utils_sql import create_connection, get_db_table, create_insert_table_db_sync
import pandas as pd
from datetime import datetime

# Список для хранения данных по каждому дню
data = []
# Создание подключения к базе данных
connection = create_connection()
# Цикл по количеству дней для расчета
days_count = 27  # Количество дней для расчета
for day in range(days_count+1):
    date = (datetime.now() - pd.Timedelta(days=day)).strftime('%Y-%m-%d')
    print(f"Выполняется расчет за дату: {date}")
    query = f"""WITH revenue AS(
	--- Расчет по выручки и количества ---
	SELECT 
		fd.account,
		SUM(fd.orders_sum) AS orders_sum,-- Выручка 16,102,468; Количество 17 138;
		SUM(fd.order_count) AS order_count,
		fd."date"
	FROM funnel_daily fd
	WHERE fd."date" = '{date}'
	GROUP BY fd."date", fd.account),
--- Расчет суммы продаж, количества ---
salec_calc AS (SELECT SUM(s.price_with_disc) AS sales_sum, -- 14,027,995.44
	COUNT(s.is_realization) AS sales_count, -- 14,776
	a.account,
	s."date" 
FROM sales s
LEFT JOIN article a 
	ON s.article_id = a.nm_id 
WHERE s."date" = '{date}'
AND s.is_realization IS TRUE
GROUP BY a.account, s."date"),
--- Закупочная стоиомость от продаж ---
    cost_price_sales AS (	
        SELECT
            COUNT(s.price_with_disc) * AVG(cp.purchase_price) AS cost_price_sales, -- 5,645,221
            a.account 
        FROM sales s
        LEFT JOIN article a 
            ON a.nm_id = s.article_id 
        LEFT JOIN (
            SELECT DISTINCT ON (cp.local_vendor_code)
                cp.local_vendor_code,
                purchase_price,
                cp.date
            FROM cost_price cp
            WHERE cp.date = '{date}'
            ORDER BY cp.local_vendor_code, cp.date DESC
    ) cp
        ON cp.local_vendor_code = a.local_vendor_code
    WHERE s."date" = '{date}'
    AND s.is_realization IS TRUE
    GROUP BY a.account),
--- Подсчет прибыли по общим условиям от заказов ---
general_cond AS (
	SELECT SUM(anpd.sum_net_profit) AS general_profit_orders, -- -233,110
	    anpd.date,
	    a.account
	FROM accurate_net_profit_data anpd
	  LEFT JOIN article a
	    ON a.nm_id = anpd.article_id
	WHERE anpd.date = '{date}'
	GROUP BY anpd.date, a.account),
--- Рекламные затраты и бонусы ---
adv_spends AS (
		SELECT 
	  SUM(CASE
	          WHEN asp.payment_type NOT IN ('Бонусы', 'Кэшбэк') THEN asp.upd_sum
	            ELSE 0
	        END) AS adv_spend,
	  SUM(CASE
	          WHEN asp.payment_type IN ('Бонусы', 'Кэшбэк') THEN asp.upd_sum
	           ELSE 0
	      END) AS bonuses,
	  ast.account
	FROM advert_spend asp
	LEFT JOIN advert_stat ast
	  ON ast.campaign_id = asp.advert_id
	  AND ast."date" = asp."date"
	WHERE asp.date = '{date}'
	GROUP BY asp.date, ast.account),
-- Закупочная стоимость от заказов ---
cost_price_orders AS(
	SELECT
	    SUM(fd.order_count * cp.purchase_price) AS cost_price_orders, -- 6 188 957
	    fd.account
	FROM funnel_daily fd
	LEFT JOIN (
	    SELECT DISTINCT ON (cp.local_vendor_code)
	           cp.local_vendor_code,
	           purchase_price,
	           cp.date
	    FROM cost_price cp
	    WHERE cp.date = '{date}'
	    ORDER BY cp.local_vendor_code, cp.date DESC
	) cp
	    ON cp.local_vendor_code = fd.wild
	   AND cp."date" = fd."date"
	WHERE fd."date" = '{date}'
	GROUP BY fd.account)
SELECT 
	UPPER(r.account) AS account,
	r.orders_sum, -- Выручка 16,102,468;
	s.sales_sum, -- 14,776
	r.orders_sum - (r.orders_sum * ((6+26)::NUMERIC/100)) - cpo.cost_price_orders AS profit_by_ind_cond_orders, -- 4,760,721.24
	round(s.sales_sum - (s.sales_sum * ((6+26)::NUMERIC/100)), 2) - cps.cost_price_sales AS profit_by_ind_cond_sales, -- 3,356,648.89
	s.sales_count, -- 14,027,995.44
	r.order_count, -- Количество 17 138;
	asp.adv_spend, -- 1,592,557
	asp.bonuses, -- 460
	(round(s.sales_sum - (s.sales_sum * ((6+26)::NUMERIC/100)), 2) - cps.cost_price_sales) - asp.adv_spend AS profit_cond_sales_minus_adv_spend,
	cpo.cost_price_orders, -- 6,188,957
	cps.cost_price_sales, -- 6 188 957
	g.general_profit_orders, -- -233,110
	r."date"
FROM revenue r
LEFT JOIN cost_price_sales cps
	ON UPPER(cps.account) = UPPER(r.account)
LEFT JOIN salec_calc s
	ON UPPER(s.account) = UPPER(r.account)
LEFT JOIN general_cond g
	ON UPPER(g.account) = UPPER(r.account)
LEFT JOIN adv_spends asp
	ON UPPER(asp.account) = UPPER(r.account)
LEFT JOIN cost_price_orders cpo
	ON UPPER(cpo.account) = UPPER(r.account);"""
    # Получение данных за один день и добавление в список
    df_one_day = get_db_table(query, connection)
    data.append(df_one_day)
# Закрытие подключения к базе данных
connection.close()


# Объединение всех данных в один датафрейм
df = pd.concat(data, ignore_index=True)

# df['profit_cond_minus_adv_spend'] = df['profit_by_cond_general_sales'] + df['bonuses'] - df['adv_spend']


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




