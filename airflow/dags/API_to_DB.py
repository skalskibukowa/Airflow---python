from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import json
import logging
import requests


# Define default_args for your DAG
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define your DAG
dag = DAG(
    'fetch_and_store_animal_facts',
    default_args=default_args,
    description='Fetch and store animal facts in PostgreSQL',
    schedule_interval='@daily',
)

# Define the function to process and store data
def process_and_store_data():

    logging.info("Starting the process_and_store_data function.")
    print("Starting the process_and_store_data function.")

    try:

        response = requests.get('https://cat-fact.herokuapp.com/facts')
        facts = response.json()

        # Process and store the data in PostgreSQL
        engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

        # Create the table if not exists
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS Animal_text (
                    text VARCHAR(255)
                )
            """)

            # Insert facts into the "Animal_text" table using parameterized query
            for fact in facts:
                text = fact.get('text', '')
                if text:
                    connection.execute("INSERT INTO Animal_text (text) VALUES (%s)", text)
    except Exception as e:
     logging.error(f"An error occurred: {str(e)}")
     print(f"An error occurred: {str(e)}")

     logging.info("Finished processing and storing data.")
     print("Starting the process_and_store_data function.")

# Define the tasks in your DAG

# Task to fetch animal facts using HTTP GET
fetch_animal_facts = SimpleHttpOperator(
    task_id='fetch_animal_facts',
    http_conn_id='http_default',  # Create a connection in Airflow with the appropriate base URL
    endpoint='/facts',
    method='GET',
    headers={"Content-Type": "application/json"},
    response_check=lambda response: True if response.status_code == 200 else False,
    dag=dag,
)

# Task to process and store data in PostgreSQL
process_and_store_task = PythonOperator(
    task_id='process_and_store_data',
    python_callable=process_and_store_data,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
fetch_animal_facts >> process_and_store_task


