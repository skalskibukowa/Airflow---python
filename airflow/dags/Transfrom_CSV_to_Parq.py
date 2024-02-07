import pyarrow.csv as pv
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG(
    'TransfromCSVtoParq_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='*/30 * * * *',
    catchup=False
)


def transfrom_csv_to_parq():
    source_csv_file_path = f"/opt/airflow/data_source/credit_card_approval.csv"
    sink_pq_file_path = f"/opt/airflow/data_sink/credit_card_approval.csv"
   
    df = pv.read_csv(source_csv_file_path)
    pq.write_table(df, sink_pq_file_path.replace('csv', 'parquet'))
    print(f'CSV file "{source_csv_file_path}" is transformed to parquet format successfully')


print_transformCSVtoParq = PythonOperator(
    task_id='transform_csv_to_parq',
    python_callable=transfrom_csv_to_parq,
    dag=dag
)

#set the dependencies between the tasks

print_transformCSVtoParq