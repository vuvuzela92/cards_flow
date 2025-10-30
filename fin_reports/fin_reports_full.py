import asyncio
import logging
from my_fin_rep_utils import fetch_all_data, load_api_tokens


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