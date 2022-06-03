from datetime import datetime
from airflow.models import DAG  # to create a DAG
from airflow.operators.python import PythonOperator
from etl import ETL

default_args = {
    "start_date": datetime(2022, 6, 3),
}
def fetch_active_etl_table():
    etl = ETL()
    etl.main()


with DAG(dag_id='etl_processing',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    #TASK

    fetch_active_tables = PythonOperator(
        task_id='fetch_active_tables',
        python_callable=fetch_active_etl_table
    )