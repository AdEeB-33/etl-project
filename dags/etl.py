from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

# DAG definition
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=datetime(2025, 1, 1),
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
    # tlTcEIOUXuscqoXHL2matZqeEKgSTi2dz5NlxIbv
    # https://api.nasa.gov/planetary/apod?api_key=tlTcEIOUXuscqoXHL2matZqeEKgSTi2dz5NlxIbv
    extract_apod = HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',  ## connection id defined in airflow for nasa api
        endpoint='planetary/apod',  ## nasa api end point for apod
        method='GET',
        data={
             "api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"
            } , # use params for GET
        response_filter=lambda response: response.json(),  # response to json
        log_response=True,
        deferrable=False
                             )

    # 3 transform the data(pick the information that i need to save)
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data

    # 4 load the data insto postgres sql
    @task
    def load_data_to_postgres(apod_data):
        # intialize postgres
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        # defining sql query injection
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s)
        """
        # execute query
        postgres_hook.run(
            insert_query,
            parameters=(
                apod_data['title'],
                apod_data['explanation'],
                apod_data['url'],
                apod_data['date'],
                apod_data['media_type']
            )
        )

    # 5 verify the data DBViewer
    # 6 define the dependecies
    # extract
    table_task = create_table() >> extract_apod  # ensure the table is created before extraction
    api_response = extract_apod.output

    # transform
    transformed_data = transform_apod_data(api_response)

    # load
    load_data_to_postgres(transformed_data)