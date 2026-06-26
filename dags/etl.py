from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import json

# DAG definition
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=days_ago(1),
    schedule='@daily',   
    catchup=False
) as dag:

    # 1. Create table if not exists
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)
      # 2 extract the nasa api data(apod) -astronomy picture of the day (extract pipeline)
      # 3 transform the data(pick the information that i need to save)
      # 4 load the data insto postgres sql
      # 5 verify the data DBViewer
      # 6 define the dependecies
