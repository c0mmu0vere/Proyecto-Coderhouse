from datetime import timedelta, datetime
from airflow.decorators import dag, task
from utils import extract_weather_data, transform_weather_data, load_weather_data
from variables import *

@dag(
    default_args={
        "owner":"Fabricio Moreira",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description = 'Agrega records de WeatherAPI a la tabla weather en Redshift',
    schedule_interval=timedelta(hours=1),
    start_date= datetime(2023, 10, 3),
    catchup=False,
    max_active_runs=1
)

def weather_ETL_DAG():

    @task
    def extract_weather_data_task():
        return extract_weather_data(provincias, api_key)

    @task
    def transform_weather_data_task(raw_df):
        return transform_weather_data(raw_df)

    @task
    def load_weather_data_task(clean_df):
        return load_weather_data(clean_df)

    extract_weather_data_results = extract_weather_data_task()
    transform_weather_data_results =  transform_weather_data_task(extract_weather_data_results)
    load_weather_data_results = load_weather_data_task(transform_weather_data_results)

    extract_weather_data_results >> transform_weather_data_results >> load_weather_data_results

Ingest_data = weather_ETL_DAG()

