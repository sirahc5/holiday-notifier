import os
import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
sys.path.insert(1, os.environ.get('DIR'))
from calendarific_etl import run_calendarific_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2021, 12, 10),
    'email': [os.environ.get('MYEMAIL')],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'calendarific_dag',
    default_args=default_args,
    description='DAG for the Calendarific project',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='whole_calendarific_etl',
    python_callable=run_calendarific_etl,
    dag=dag
)

run_etl
