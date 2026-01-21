# %%
from utils_sql import create_connection, get_db_table, create_insert_table_db_sync
import pandas as pd
from datetime import datetime

# %%
# Список для хранения данных по каждому дню
data = []
# Создание подключения к базе данных
connection = create_connection()
# Цикл по количеству дней для расчета
days_count = 1  # Количество дней для расчета
for day in range(days_count+1):
    date = (datetime.now() - pd.Timedelta(days=day+1)).strftime('%Y-%m-%d')
    print(f"Выполняется расчет за дату: {date}")
    query = f"""-- Условынй расчет 2.0
    -- 1. Вычисляем прибыль от заказов
    WITH profit_orders_by_cond AS( 
    SELECT 
    --		ROUND(ROUND(AVG(price_with_disc))-(ROUND(AVG(price_with_disc))/100*(6+AVG(ic2025.fbs_individual_conditions))) - AVG(cp.purchase_price)) AS profit_by_cond_unit_orders, -- Цена для клиента - (Цена для клиента/100*(налог+комиссия по ИУ)) - закупочная стоимость
    --		ROUND(ROUND(AVG(price_with_disc))-(ROUND(AVG(price_with_disc))/100*(6+AVG(ic2025.fbs_individual_conditions))) - AVG(cp.purchase_price)) * AVG(r.orders_count) AS profit_by_cond_general_orders, -- прибыль по ИУ с заказов
            -- Cчитаем данные по прибыли в зависимости от года
            CASE
                WHEN o."date" < '2026-01-01' THEN ROUND(ROUND(AVG(price_with_disc))-(ROUND(AVG(price_with_disc))/100*(6+AVG(ic2025.fbs_individual_conditions))) - AVG(cp.purchase_price))  -- Цена для клиента - (Цена для клиента/100*(налог+комиссия по ИУ)) - закупочная стоимость
                WHEN o."date" > '2026-01-01'THEN ROUND(ROUND(AVG(price_with_disc))-(ROUND(AVG(price_with_disc))/100*(6+AVG(ic2026.fbs_individual_conditions))) - AVG(cp.purchase_price))
            END AS profit_by_cond_unit_orders,
            CASE
                WHEN o."date" < '2026-01-01' THEN ROUND(ROUND(AVG(price_with_disc))-(ROUND(AVG(price_with_disc))/100*(6+AVG(ic2025.fbs_individual_conditions))) - AVG(cp.purchase_price)) * AVG(r.orders_count) -- Цена для клиента - (Цена для клиента/100*(налог+комиссия по ИУ)) - закупочная стоимость
                WHEN o."date" > '2026-01-01'THEN ROUND(ROUND(AVG(price_with_disc))-(ROUND(AVG(price_with_disc))/100*(6+AVG(ic2026.fbs_individual_conditions))) - AVG(cp.purchase_price)) * AVG(r.orders_count)
            END AS profit_by_cond_general_orders,
            AVG(ic2025.fbs_individual_conditions) fbs_individual_conditions_2025,
            AVG(ic2025.fbs_individual_conditions) fbs_individual_conditions_2026,
            o.article_id,
            AVG(r.orders_sum_rub ) AS revenue, -- выручка от воронки
            AVG(r.orders_count) AS orders_count, -- количество заказов
            AVG(r.orders_count) * AVG(cp.purchase_price) AS purchase_orders_price,
            o."date" AS date,
            a.account 
        FROM orders o
        LEFT JOIN article a ON
        a.nm_id = o.article_id
        LEFT JOIN (SELECT cp.local_vendor_code, -- Добавляю данные по закупочной стоимости
            cp.purchase_price AS purchase_price
        FROM cost_price cp
        WHERE cp."date" = '{date}') cp ON
        cp.local_vendor_code = a.local_vendor_code
        LEFT JOIN (SELECT -- Добавляю данные воронки продаж
            r.article_id,
            r.orders_sum_rub,
            r.orders_count 
            FROM orders_revenues r
            WHERE r."date" = '{date}') r ON 
        r.article_id = o.article_id
        LEFT JOIN(SELECT -- Данные по ИУ 2025
                ic.subject_name,
                cd.article_id,
                ic.fbo_individual_conditions,
                ic.fbs_individual_conditions,
                ic.date_from 
            FROM individual_conditions ic
            LEFT JOIN card_data cd 
                USING(subject_name)
            WHERE ic.date_from <= '2026-01-01') ic2025
        ON ic2025.article_id = o.article_id 
        LEFT JOIN(SELECT -- Данные по ИУ 2025
                ic.subject_name,
                cd.article_id,
                ic.fbo_individual_conditions,
                ic.fbs_individual_conditions,
                ic.date_from 
            FROM individual_conditions ic
            LEFT JOIN card_data cd 
                USING(subject_name)
            WHERE ic.date_from >= '2026-01-01') ic2026
        ON ic2026.article_id = o.article_id 
        WHERE o."date" = '{date}'
        GROUP BY o.article_id, a.account, o."date"),
    revenue_funnel AS( -- Вычисляю прибыль по всем заказам по ИУ
    SELECT 
            SUM(profit_by_cond_general_orders) AS profit_by_cond_general_orders,
            account
        FROM profit_orders_by_cond
        GROUP BY account),
    sales_filtered AS(SELECT 
        s.article_id,
        s.price_with_disc,
        CASE
            WHEN s.is_realization IS TRUE THEN 1
            WHEN s.is_realization IS FALSE THEN 0
            ELSE NULL  
        END AS is_buyot,
        o."date" AS order_date,
        s."date"
    FROM sales s
    LEFT JOIN orders o
    USING(srid)),
    calculate_sales_profit AS( -- вычисление данных по продажам
        SELECT 
        SUM(s.price_with_disc) AS sum_sales,
        ROUND(ROUND(AVG(price_with_disc))-(ROUND(AVG(price_with_disc))/100*(6+26)) - AVG(cp.purchase_price)) * SUM(s.is_buyot) AS profit_by_cond_general_sales, -- прибыль по ИУ с заказов
        SUM(s.is_buyot) AS buyot_count,
        AVG(cp.purchase_price) * SUM(s.is_buyot) AS purchase_sales_price, -- закупочная стоимость от продаж
        ROUND(SUM(is_buyot) * AVG(anpd.net_profit)) AS non_cond_profit, -- Прибыль по общим условиям
        a.account 
    FROM sales_filtered s
    LEFT JOIN article a 
    ON s.article_id = a.nm_id 
    LEFT JOIN (SELECT cp.local_vendor_code,
        cp.purchase_price AS purchase_price
    FROM cost_price cp
    WHERE cp."date" = '{date}') cp ON
    cp.local_vendor_code = a.local_vendor_code
    LEFT JOIN (
        SELECT anpd.article_id,
            SUM(anpd.net_profit) AS net_profit
            FROM accurate_net_profit_data anpd
            WHERE anpd."date" = '{date}'
            GROUP BY anpd.article_id) anpd
        ON anpd.article_id = s.article_id 
    WHERE s."date" = '{date}'
    GROUP BY a.account),
    calculate_adv_spend AS (SELECT -- вычисление рекламных затрат
        SUM(
            CASE
                WHEN sp.payment_type NOT IN ('Бонусы', 'Кэшбэк') THEN sp.upd_sum
                ELSE 0
            END
        ) AS adv_spend,
        SUM(
            CASE
                WHEN sp.payment_type IN ('Бонусы', 'Кэшбэк') THEN sp.upd_sum
                ELSE 0
            END
        ) AS bonuses,
        st.account
    FROM advert_spend sp
    LEFT JOIN (
        SELECT DISTINCT UPPER(st.account) AS account,
                        st.campaign_id
        FROM advert_stat st
    ) st ON st.campaign_id = sp.advert_id
    WHERE sp."date" = '{date}'
    GROUP BY st.account)
    SELECT 
        account,
        COALESCE(SUM(p.revenue), 0) AS revenue, -- выручка
        COALESCE(AVG(s.sum_sales), 0) AS sum_sales, -- сумма заказов за день
        COALESCE(SUM(p.profit_by_cond_general_orders), 0) AS profit_by_cond_general_orders, -- прибыль по ИУ от заказов
        COALESCE(AVG(s.profit_by_cond_general_sales), 0) AS profit_by_cond_general_sales, -- прибыль от продаж по ИУ
        COALESCE(AVG(s.buyot_count), 0) AS buyot_count, -- количество продаж в день
        COALESCE(SUM(p.orders_count), 0) AS orders_count, -- количество заказов в день
        COALESCE(AVG(sp.adv_spend), 0) AS adv_spend, -- Рекламные затраты
        COALESCE(AVG(sp.bonuses), 0) AS bonuses, -- Рекламные бонусы
        COALESCE(AVG(s.profit_by_cond_general_sales) + AVG(sp.bonuses) - AVG(sp.adv_spend), 0) AS profit_cond_minus_adv_spend, -- Прибыль от продаж за минусом рекламы
        COALESCE(SUM(p.purchase_orders_price), 0) AS purchase_orders_price, -- закупочная стоимость от заказов
        COALESCE(AVG(s.purchase_sales_price), 0) AS purchase_sales_price, -- закупочная стоимость от продаж
        COALESCE(AVG(s.non_cond_profit), 0) AS non_cond_profit, -- Прибыль по общим условиям
        COALESCE(ROUND(((AVG(s.profit_by_cond_general_sales) + AVG(sp.bonuses) - AVG(sp.adv_spend))/SUM(p.revenue)) * 100, 2), 0) AS profit_percent,
        p.date -- дата
    FROM profit_orders_by_cond p
    LEFT JOIN revenue_funnel r
        USING(account)
    LEFT JOIN calculate_sales_profit s
        USING(account)
    LEFT JOIN calculate_adv_spend sp
        USING(account)
    GROUP BY p.account, p.date;"""
    # Получение данных за один день и добавление в список
    df_one_day = get_db_table(query, connection)
    data.append(df_one_day)
# Закрытие подключения к базе данных
connection.close()

# %%
# Объединение всех данных в один датафрейм
df = pd.concat(data, ignore_index=True)
df['profit_cond_minus_adv_spend'] = df['profit_by_cond_general_sales'] + df['bonuses'] - df['adv_spend']


table_name = 'conditions_calculation'
columns_type = {
    'account': 'VARCHAR(255)',
    'revenue': 'BIGINT',
    'sum_sales': 'BIGINT',
    'profit_by_cond_general_orders': 'BIGINT',
    'profit_by_cond_general_sales': 'BIGINT',
    'buyot_count': 'BIGINT',
    'orders_count': 'BIGINT',
    'adv_spend': 'BIGINT',
    'bonuses': 'BIGINT',
    'profit_cond_minus_adv_spend': 'BIGINT',
    'purchase_orders_price': 'BIGINT',
    'purchase_sales_price': 'BIGINT',
    'non_cond_profit': 'BIGINT',
    'profit_percent': 'NUMERIC(10,2)',
    'date': 'DATE'
}
# Ключевые колонки для UPSERT
key_columns = ('account', 'date')  # как первичный ключ
create_insert_table_db_sync(df, table_name, columns_type, key_columns)
print(f"Данные добавлены в БД {table_name}")




