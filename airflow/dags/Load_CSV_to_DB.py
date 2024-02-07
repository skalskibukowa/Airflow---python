import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def ingest_csv_to_postgres(csv_path, table_name, database_url):
    # Load CSV file into a DataFrame
    df = pd.read_csv(csv_path)

    # Get the current date and time for creating a unique table name
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_table_name = f"{table_name}_{current_datetime}"

    # Add 'Source' column with the name of the file
    df['Source'] = csv_path.split('/')[-1]

    # Connect to PostgreSQL database
    engine = create_engine(database_url)

    # Dynamically create the table in PostgreSQL
    df.head(0).to_sql(name=unique_table_name, con=engine, index=False, if_exists='replace')

    # Write the DataFrame to PostgreSQL
    df.to_sql(name=unique_table_name, con=engine, index=False, if_exists='append')

    print(f'Data from CSV file "{csv_path}" ingested into table "{unique_table_name}" in PostgreSQL.')

# Example usage
current_datetime = datetime.now().strftime("%Y-%m-%d")
csv_file_path = f"/opt/airflow/data_sink/sales_data_{current_datetime}.csv"
postgres_table_name = "Sales"
postgres_database_url = "postgresql://airflow:airflow@postgres:5432/airflow"

dag = DAG(
    'LoadCSVtoDB_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='*/2 * * * *',
    catchup=False
)

print_IngestCSV = PythonOperator(
    task_id='print_IngestCSV',
    python_callable=ingest_csv_to_postgres,
    op_kwargs={'csv_path': csv_file_path, 'table_name': postgres_table_name, 'database_url': postgres_database_url},
    dag=dag
)

# Set the dependencies between the tasks
print_IngestCSV
