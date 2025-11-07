import asyncio
import logging
from my_fin_rep_utils import fetch_all_data, load_api_tokens
from utils_sql import create_connection_to_vector_db, execute_query


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('fin_reports.log'),  # Логи будут записываться в файл fin_reports.log
        logging.StreamHandler()  # Логи также будут выводиться в консоль
    ]
)

if __name__ == "__main__":
    asyncio.run(fetch_all_data(load_api_tokens(), num_weeks=1))
    logging.info("✅ Загрузка данных завершена")
    # Устанавливаем соединение с БД
    connection = create_connection_to_vector_db()
    # Обновляем материализованные представления
    query_penalties = """REFRESH MATERIALIZED VIEW CONCURRENTLY public.penalties_mv;"""
    execute_query(connection, query_penalties)
    logging.info("Материализованное представление penalties_mv 🔁")

    query_fin_rep = """REFRESH MATERIALIZED VIEW CONCURRENTLY public.fin_reports_mv;"""
    execute_query(connection, query_fin_rep)
    logging.info("Материализованное представление fin_reports_mv 🔁")

    query_fin_deductions = """REFRESH MATERIALIZED VIEW CONCURRENTLY public.fin_deductions_mv;"""
    execute_query(connection, query_fin_rep)
    logging.info("Материализованное представление fin_deductions_mv 🔁")

    query_fin_weekly_fin_rep = """REFRESH MATERIALIZED VIEW CONCURRENTLY public.weekly_fin_reports_mv;"""
    execute_query(connection, query_fin_rep)
    logging.info("Материализованное представление weekly_fin_reports_mv 🔁")
