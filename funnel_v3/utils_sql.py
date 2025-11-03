"""Функции для работы с БД"""
import psycopg2
from psycopg2 import OperationalError
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Подключение к базе данных
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print(f"Соединение с БД PostgreSQL успешно установлено в {datetime.now().strftime('%Y-%m-%d-%H:M')}")
    except OperationalError as error:
        print(f"Произошла ошибка при подключении к БД PostgreSQL {error}")
    return connection

# Исполнение SQL запросов
def execute_query(connection, query, data=None):
    cursor = connection.cursor()
    try:
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
        connection.commit()  # явное подтверждение транзакции
        print(f"Запрос успешно выполнен в {datetime.now().strftime('%Y-%m-%d')}")
    except Exception as e:
        connection.rollback()  # откат транзакции в случае ошибки
        print(f"Ошибка выполнения запроса: {e}")
    finally:
        cursor.close()

# Функция на чтение данных из БД
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as error:
        print(f'Произошла ошибка при выводе данных {error}')

# Функция для получения датафрейма из БД
def get_db_table(db_query: str, connection):
    """Функция получает данные из Базы Данных и преобразует их в датафрейм"""
    execute_read_query(connection, db_query)
    # Преобразуем таблицу в датафрейм
    try:
        df_db = pd.read_sql(db_query, connection).fillna(0).infer_objects(copy=False)
        print('Данные из БД загружены в датафрейм')
        return df_db
    except Exception as e:
        print(f'Ошибка получения данных из БД {e}')