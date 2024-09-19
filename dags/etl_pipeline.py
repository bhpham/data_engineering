from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Define the functions for each task
def fetch_data():
    # Simulate fetching data and saving to CSV
    data = {
        'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'quantity': [10, 20, 15],
        'price': [5.0, 10.0, 7.5]
    }
    df = pd.DataFrame(data)
    df.to_csv('/tmp/sales_data.csv', index=False)

def process_data():
    # Load the CSV file
    df = pd.read_csv('/tmp/sales_data.csv')
    # Data cleaning
    df['total_sales'] = df['quantity'] * df['price']
    df['avg_sales'] = df['total_sales'] / df['quantity']
    
    # Save the transformed data
    df.to_csv('/tmp/sales_data_processed.csv', index=False)

# Define the DAG
with DAG('etl_pipeline', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    task1 = PythonOperator(task_id='fetch_data', python_callable=fetch_data)
    task2 = PythonOperator(task_id='process_data', python_callable=process_data)

    task1 >> task2  # Set task dependencies

