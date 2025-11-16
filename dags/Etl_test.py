from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator

#!/usr/bin/env python3 -u

#from scripts.test import cr

import pandas as pd

a="s"
def cr():
    print('ok'+ a)
    #create_table(create_table_sql)

default_args = {
    'owner': 'Mikhail',
    'start_date': days_ago(0),
    'depends_on_past': False,
}

def my_callable(*args, **kwargs):
    print("Hello from my_callable")

with DAG(
    'etl_test_py',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id='echo_hi',
        bash_command='echo "Hello"',
    )
    t2 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
    t3 = BashOperator(
        task_id='print_date1',
        bash_command='date',
    )
    t4 = BashOperator(
        task_id='print_date2',
        bash_command='date',
    )
    t5 = PythonOperator(
        task_id='PythonTask',
        python_callable=my_callable
    )
    t1 >> t2
    t1 >> t3
    t1 >> t5
    t2 >> t4
    t3 >> t4
