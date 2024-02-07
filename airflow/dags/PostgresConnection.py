from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='dag_with_postgres_operator_v03',
    default_args=default_args,
    start_date=datetime(2021, 12, 19),
    schedule_interval='*/2 * * * *'
)


task1 = PostgresOperator(
    task_id='create_postgres_table',
    postgres_conn_id='Postgres_Airflow',
    sql="""
        create table if not exists dag_runs (
            dt date,
            dag_id character varying,
            primary key (dt, dag_id)
        )
    """,
    dag=dag
)

# Set the dependencies between tasks
task1