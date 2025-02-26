import json
import logging
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from s3_to_postgres import S3ToPostgresOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
}

def _fetch_weather_data(**context):
    """Fetches data from WeatherBit API and save it to S3.
    """
    logging.info(f"Fetching weather data")
    api_key = Variable.get("WeatherBitApiKey")
    full_url = f"https://api.weatherbit.io/v2.0/current?city=Paris&country=France&key={api_key}"
    response = requests.get(full_url)
    filename = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_weather_data.json"
    full_path_to_file = f"/tmp/{filename}"
    with open(full_path_to_file, "w") as f:
        json.dump(response.json(), f)

    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_file(filename=full_path_to_file, key=filename, bucket_name=Variable.get("S3BucketName"))

    context["task_instance"].xcom_push(key="weather_filename", value=filename)
    logging.info(f"Saved weather data to {filename}")


def _transform_weather_data(**context):
    """Transforms raw data from JSON file to ingestable data for Postgres.
    """

    filename = context["task_instance"].xcom_pull(key="weather_filename")

    s3_hook = S3Hook(aws_conn_id="aws_default")
    returned_filename = s3_hook.download_file(filename, bucket_name=Variable.get("S3BucketName"), local_path="/tmp")
    with open(returned_filename, "r") as f:
        raw_data_json = json.load(f)

    raw_data_json = raw_data_json["data"][0]
    transformed_data = {
        "observation_time": raw_data_json["ob_time"],
        "pressure": raw_data_json["pres"],
        "wind_speed": raw_data_json["wind_spd"],
        "wind_dir_angle": raw_data_json["wind_dir"],
        "wind_dir_letter": raw_data_json["wind_cdir_full"],
        "temperature": raw_data_json["temp"],
        "apparent_temp": raw_data_json["app_temp"],
        "humidity": raw_data_json["rh"],
        "precipitation": raw_data_json["precip"],
        "snow": raw_data_json["snow"],
        "uv_index": raw_data_json["uv"],
        "air_quality_index": raw_data_json["aqi"],
        "cloud_coverage": raw_data_json["clouds"],
        "description": raw_data_json["weather"]["description"],
    }
    df = pd.DataFrame(transformed_data, index=[0])
    csv_filename = filename.split(".")[0] + ".csv"
    csv_filename_full_path = f"/tmp/{csv_filename}"
    df.to_csv(csv_filename_full_path, index=False, header=False)
    s3_hook.load_file(filename=csv_filename_full_path, key=csv_filename, bucket_name=Variable.get("S3BucketName"))
    context["task_instance"].xcom_push(key="weather_csv_filename", value=csv_filename)

    
def _fetch_status_data(**context):
    """Fetches station status data from Velib Metropole.
    """
    logging.info(f"Fetching status station data")
    response = requests.get(f"https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json")
    filename = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_status_data.json"
    full_path_to_file = f"/tmp/{filename}"
    with open(full_path_to_file, "w") as f:
        json.dump(response.json(), f)
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_file(filename=full_path_to_file, key=filename, bucket_name=Variable.get("S3BucketName"))
    context["task_instance"].xcom_push(key="status_filename", value=filename)
    logging.info(f"Saved status station data to {filename}")


def _transform_status_data(**context):
    """Transforms raw data from JSON file to ingestable data for Postgres.
    """
    filename = context["task_instance"].xcom_pull(key="status_filename")
    s3_hook = S3Hook(aws_conn_id="aws_default")
    returned_filename = s3_hook.download_file(filename, bucket_name=Variable.get("S3BucketName"), local_path="/tmp")
    with open(returned_filename, "r") as f:
        raw_data_json = json.load(f)
    df = pd.json_normalize(raw_data_json["data"]["stations"])
    df = df[["station_id", "last_reported", "numBikesAvailable", "numDocksAvailable"]]
    df["last_reported"] = df["last_reported"].apply(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))
    csv_filename = filename.split(".")[0] + ".csv"
    csv_filename_full_path = f"/tmp/{csv_filename}"
    df.to_csv(csv_filename_full_path, index=False, header=False)
    s3_hook.load_file(filename=csv_filename_full_path, key=csv_filename, bucket_name=Variable.get("S3BucketName"))
    context["task_instance"].xcom_push(key="status_csv_filename", value=csv_filename)


with DAG(dag_id="etl_dag", default_args=default_args, schedule_interval="@hourly", catchup=False) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup(group_id="weather_branch") as weather_branch:
        fetch_weather_data = PythonOperator(task_id="fetch_weather_data", python_callable=_fetch_weather_data)

        transform_weather_data = PythonOperator(
            task_id="transform_weather_data",
            python_callable=_transform_weather_data
        )

        create_weather_table = PostgresOperator(
            task_id="create_weather_table",
            sql="""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                observation_time TIMESTAMP,
                pressure DECIMAL(5, 2),
                wind_speed DECIMAL(5, 2),
                wind_dir_angle DECIMAL(5, 2),
                wind_dir_letter VARCHAR,
                temperature DECIMAL(5, 2),
                apparent_temp DECIMAL(5, 2),
                humidity DECIMAL(5, 2),
                precipitation DECIMAL(5, 2),
                snow DECIMAL(5, 2),
                uv_index DECIMAL(5, 2),
                air_quality_index DECIMAL(5, 2),
                cloud_coverage DECIMAL(5, 2),
                description VARCHAR
            )
            """,
            postgres_conn_id="postgres_default",
        )

        transfer_weather_data_to_postgres = S3ToPostgresOperator(
            task_id="transfer_weather_data_to_postgres",
            table="weather_data",
            bucket="{{ var.value.S3BucketName }}",
            key="{{ task_instance.xcom_pull(key='weather_csv_filename') }}",
            postgres_conn_id="postgres_default",
            aws_conn_id="aws_default",
        )

        fetch_weather_data >> transform_weather_data >> create_weather_table >> transfer_weather_data_to_postgres

    with TaskGroup(group_id="status_branch") as status_branch:
        fetch_status_data = PythonOperator(task_id="fetch_status_data", python_callable=_fetch_status_data)

        transform_status_data = PythonOperator(task_id="transform_status_data", python_callable=_transform_status_data)

        create_status_table = PostgresOperator(
            task_id="create_status_table",
            sql="""
            CREATE TABLE IF NOT EXISTS status_station (
                id SERIAL PRIMARY KEY,
                station_id VARCHAR,
                last_reported TIMESTAMP,
                bikes_available INTEGER,
                docks_available INTEGER
            )
            """,
            postgres_conn_id="postgres_default",
        )

        transfer_status_data_to_postgres = S3ToPostgresOperator(
            task_id="transfer_status_data_to_postgres",
            table="status_station",
            bucket="{{ var.value.S3BucketName }}",
            key="{{ task_instance.xcom_pull(key='status_csv_filename') }}",
            postgres_conn_id="postgres_default",
            aws_conn_id="aws_default",
        )

        fetch_status_data >> transform_status_data >> create_status_table >> transfer_status_data_to_postgres

    end = DummyOperator(task_id="end")

    start >> [weather_branch, status_branch] >> end
