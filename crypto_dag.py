from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from crypto_etl import run_crypto_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 3),
    'email': ['airflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'cryptocurrency_dag',
    default_args=default_args,
    description='DAG with ETL process',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_cryptocurrency_etl',
    python_callable=run_crypto_etl,
    dag=dag, 
)

run_etl
