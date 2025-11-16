from datetime import datetime, timedelta
import requests
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

logger = logging.getLogger("airflow.task")

def fetch_iss_data():
    try:
        logger.info("Получение данных о положении МКС")
        response = requests.get('http://api.open-notify.org/iss-now.json', timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Данные получены: {data}")
        return data
    except Exception as e:
        logger.error(f"Ошибка при получении данных: {str(e)}")
        raise

def init_postgres_connection():
    try:
        logger.info("Инициализация подключения к PostgreSQL")
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        conn = pg_hook.get_conn()
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {str(e)}")
        raise

def create_table_if_not_exists():
    try:
        logger.info("Создание таблицы (если не существует)")
        conn = init_postgres_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS iss_position (
                    timestamp TIMESTAMP PRIMARY KEY,
                    latitude FLOAT NOT NULL,
                    longitude FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
        logger.info("Таблица успешно создана/проверена")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def save_iss_data(ti):
    try:
        data = ti.xcom_pull(task_ids='fetch_iss_data')
        if not data:
            raise ValueError("Данные не получены")
            
        logger.info(f"Сохранение данных: {data}")
        
        conn = init_postgres_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO iss_position (timestamp, latitude, longitude)
                VALUES (to_timestamp(%s), %s, %s)
                ON CONFLICT (timestamp) DO NOTHING;
            """, (
                data['timestamp'],
                data['iss_position']['latitude'],
                data['iss_position']['longitude']
            ))
            conn.commit()
        logger.info("Данные успешно сохранены")
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

with DAG(
    'iss_position_tracking_fixed',
    default_args=default_args,
    description='Отслеживание положения МКС с исправленным подключением к PostgreSQL',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    tags=['space', 'iss'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_iss_data',
        python_callable=fetch_iss_data
    )

    create_table_task = PythonOperator(
        task_id='create_table_if_not_exists',
        python_callable=create_table_if_not_exists
    )

    save_task = PythonOperator(
        task_id='save_iss_data',
        python_callable=save_iss_data
    )

    create_table_task >> fetch_task >> save_task