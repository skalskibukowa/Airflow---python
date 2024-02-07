from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import pandas as pd

# default_args for your DAG
default_args = {
    'owner': 'Team 1',
    "depends_on_past": False,
    'start_date': datetime(2023,11,18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define your DAG

dag = DAG(
    'etl_sales_data',
    default_args=default_args,
    description='A simple ETL process for the sales table',
    schedule_interval='@daily' 
)

# Define the transformation function

def transform_data(**kwargs):
    execution_date = kwargs['execution_date']
    source_table = f"sales_{execution_date.strftime('%Y%m%d_%H%M%S')}"

    source_engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
    target_engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow_2")

    # Load data from source
    query = "SELECT * FROM public.\"Sales_20231119_191010\""
    df = pd.read_sql(query, source_engine)

    # Transform data
    df['price'] = df['price'] * 1.25

    # Ingest transformed data into target
    df.to_sql('sales', target_engine, if_exists='replace', index=False)

# Define the tasks in your DAG

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)


transform_data