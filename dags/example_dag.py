from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task():
    print("Hello, Airflow!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(dag_id='my_dag', default_args=default_args, schedule_interval='@daily') as dag:
    task = PythonOperator(
        task_id='my_task',
        python_callable=my_task,
    )
