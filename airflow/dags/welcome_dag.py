from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
from datetime import datetime
import requests

import os

#get dag directory path
dag_path = os.getcwd()

def print_welcome():
    print('Welcome to Airflow!')
    print("Current Working Directory:", os.getcwd())


def print_date():
    print('Today is {}'.format(datetime.today().date()))


def print_random_quote():
    response = requests.get('https://api.quotable.io/random')
    quote = response.json()['content']
    print('Quote of the day: "{}"'.format(quote))

def process_csv_file():

    #Input file
    input_file_path = f"/opt/airflow/data_source/customer_data.csv"

    # Load CSV file into a DataFrame
    df = pd.read_csv(input_file_path)

    #Double the values in the desired column (e.g. 'column_to_double)
    df['age'] = df['age'] * 2

    # Get the current date and time in a specific format (e.g., YYYY-MM-DD_HHMMSS)
    current_datetime = datetime.now().strftime("%Y-%m-%d")

    # Save the modified DataFrame to a new CSV file with a dynamic name
    output_file_path = f"/opt/airflow/data_sink/customer_data_{current_datetime}.csv"
    df.to_csv(output_file_path, index=False)
    print('CSV file processed and saved to {}'.format(output_file_path))


def process_csv_file_sales():

    #Input file
    input_file_path = f"/opt/airflow/data_source/sales_data.csv"

    #Load CSV file into a DataFrame
    df = pd.read_csv(input_file_path)

    #Double the value of quantity
    df['quantity'] = df['quantity'] * 2
    df['price'] = df['price'] * 1.25

    # Get the current date and time in a specific format (e.g., YYYY-MM-DD_HHMMSS)
    current_datetime = datetime.now().strftime("%Y-%m-%d")

    # Save the modified DataFrame to a new CSV file with a dynamic name
    output_file_path = f"/opt/airflow/data_sink/sales_data_{current_datetime}.csv"
    df.to_csv(output_file_path, index=False)
    print('CSV file processed and saved to {}'.format(output_file_path))


def merge_sales_customer_csv_file():

    # Get the current date and time in a specific format (e.g., YYYY-MM-DD_HHMMSS)
    current_datetime = datetime.now().strftime("%Y-%m-%d")

    #input file
    input_file_path_customer = f"/opt/airflow/data_sink/customer_data_{current_datetime}.csv"
    input_file_path_sales = f"/opt/airflow/data_sink/sales_data_{current_datetime}.csv"

    #Load CSV file into a DataFrame
    df_customer = pd.read_csv(input_file_path_customer)
    df_sales = pd.read_csv(input_file_path_sales)

    merged_df = df_customer.merge(df_sales, how='left', on='customer_id')

    # Save the modified DataFrame to a new CSV file with a dynamic name
    output_file_path = f"/opt/airflow/data_sink/CustomerSales_data_{current_datetime}.csv"
    merged_df.to_csv(output_file_path, index=False)
    print('CSV file processed and saved to {}'.format(output_file_path))


dag = DAG(
    'welcome_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='*/2 * * * *',
    catchup=False
)

print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag
)


print_random_quote = PythonOperator(
    task_id='print_random_quote',
    python_callable=print_random_quote,
    dag=dag
)

print_csv_task = PythonOperator(
    task_id='print_csv_task',
    python_callable=process_csv_file,
    dag=dag
)

print_csv_task_sales = PythonOperator(
    task_id='print_csv_task_sales',
    python_callable=process_csv_file_sales,
    dag=dag
)

print_csv_taks_CustomerSales = PythonOperator(
    task_id='print_csv_taks_CustomerSales',
    python_callable=merge_sales_customer_csv_file,
    dag=dag
)


# Set the dependencies between the tasks
print_welcome_task >> print_date_task >> [print_csv_task,print_csv_task_sales]  >> print_random_quote >> print_csv_taks_CustomerSales