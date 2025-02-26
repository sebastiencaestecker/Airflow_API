"""
ETL DAG using Redshift instead of Postgres.

To use this DAG, you need to set some variables within the Airflow UI:

- `WeatherBitApiKey`: the API key to use to access the WeatherBit API.
"""
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
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
}

def _fetch_weather_data(**context):
    """Fetches data from WeatherBit API and save it to S3.
    """
    logging.info(f"Fetching weather data")
    # Get the API key from the Variables
    api_key = Variable.get("WeatherBitApiKey")
    # Fetch WeatherBit
    full_url = f"https://api.weatherbit.io/v2.0/current?city=Paris&country=France&key={api_key}"
    response = requests.get(full_url)
    # We create a filename like: 20220601-123000_weather_data.json
    filename = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_weather_data.json"
    # Let's temprorary save this file into /tmp folder
    full_path_to_file = f"/tmp/{filename}"
    with open(full_path_to_file, "w") as f:
        json.dump(response.json(), f)
    # Connect to our S3 bucket and load the file
    # filename is the path to our file and key is the full path inside the
    # bucket
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_file(filename=full_path_to_file, key=filename, bucket_name=Variable.get("S3BucketName"))
    # Let's push the filename to the context so that we can use it later
    context["task_instance"].xcom_push(key="weather_filename", value=filename)
    logging.info(f"Saved weather data to {filename}")


def _transform_weather_data(**context):
    """Transforms raw data from JSON file to ingestable data for Redshift.
    """
    # We get the filename from the context
    filename = context["task_instance"].xcom_pull(key="weather_filename")
    # Connect to our S3 bucket and download the JSON file
    s3_hook = S3Hook(aws_conn_id="aws_default")
    returned_filename = s3_hook.download_file(filename, bucket_name=Variable.get("S3BucketName"), local_path="/tmp")
    with open(returned_filename, "r") as f:
        raw_data_json = json.load(f)
    # We transform the data into a pandas DataFrame
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
    # Keep the same filename between the JSON file and the CSV
    csv_filename = filename.split(".")[0] + ".csv"
    csv_filename_full_path = f"/tmp/{csv_filename}"
    # Save it temporarily in /tmp folder
    df.to_csv(csv_filename_full_path, index=False, header=False)
    # Load it to S3
    s3_hook.load_file(filename=csv_filename_full_path, key=csv_filename, bucket_name=Variable.get("S3BucketName"))
    # Push the filename to the context so that we can use it later
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
    """Transforms raw data from JSON file to ingestable data for Redshift.
    """
    filename = context["task_instance"].xcom_pull(key="status_filename")
    s3_hook = S3Hook(aws_conn_id="aws_default")
    returned_filename = s3_hook.download_file(filename, bucket_name=Variable.get("S3BucketName"), local_path="/tmp")
    with open(returned_filename, "r") as f:
        raw_data_json = json.load(f)
    # Here the process is a bit different because we have multiple stations.
    # pd.json_normalize() will transform the data into a pandas DataFrame and
    # will flatten the data for us!
    # We just need to give it the array of dict, which is inside "data" and
    # "stations" keys.
    df = pd.json_normalize(raw_data_json["data"]["stations"])
    # We keep only those four columns
    df = df[["station_id", "last_reported", "numBikesAvailable", "numDocksAvailable"]]
    # We convert "last_reported" column to datetime
    df["last_reported"] = df["last_reported"].apply(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))
    csv_filename = filename.split(".")[0] + ".csv"
    csv_filename_full_path = f"/tmp/{csv_filename}"
    df.to_csv(csv_filename_full_path, index=False, header=False)
    s3_hook.load_file(filename=csv_filename_full_path, key=csv_filename, bucket_name=Variable.get("S3BucketName"))
    context["task_instance"].xcom_push(key="status_csv_filename", value=csv_filename)


with DAG(dag_id="etl_dag_redshift", default_args=default_args, schedule_interval="@hourly", catchup=False) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup(group_id="weather_branch") as weather_branch:
        fetch_weather_data = PythonOperator(task_id="fetch_weather_data", python_callable=_fetch_weather_data)

        transform_weather_data = PythonOperator(
            task_id="transform_weather_data",
            python_callable=_transform_weather_data
        )

        create_weather_redshift_table = RedshiftSQLOperator(
            task_id="create_weather_redshift_table",
            # In the SQL do not forget to put `IF NOT EXISTS`
            sql="""
            CREATE TABLE IF NOT EXISTS weather_data (
                id BIGINT IDENTITY(0, 1) PRIMARY KEY,
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
            redshift_conn_id="redshift_default",
        )

        transfer_weather_data_to_redshift = S3ToRedshiftOperator(
            task_id="transfer_weather_data_to_redshift",
            schema="PUBLIC",
            table="weather_data",
            s3_bucket="{{ var.value.S3BucketName }}",
            s3_key="{{ task_instance.xcom_pull(key='weather_csv_filename') }}",
            redshift_conn_id="redshift_default",
            aws_conn_id="aws_default",
            copy_options=["csv"],
        )

        fetch_weather_data >> transform_weather_data >> create_weather_redshift_table >> transfer_weather_data_to_redshift

    with TaskGroup(group_id="status_branch") as status_branch:
        fetch_status_data = PythonOperator(task_id="fetch_status_data", python_callable=_fetch_status_data)

        transform_status_data = PythonOperator(task_id="transform_status_data", python_callable=_transform_status_data)

        create_status_redshift_table = RedshiftSQLOperator(
            task_id="create_status_redshift_table",
            sql="""
            CREATE TABLE IF NOT EXISTS status_station (
                id BIGINT IDENTITY(0, 1) PRIMARY KEY,
                station_id VARCHAR,
                last_reported TIMESTAMP,
                bikes_available INTEGER,
                docks_available INTEGER
            )
            """,
            redshift_conn_id="redshift_default",
        )

        transfer_status_data_to_redshift = S3ToRedshiftOperator(
            task_id="transfer_status_data_to_redshift",
            schema="PUBLIC",
            table="status_station",
            s3_bucket="{{ var.value.S3BucketName }}",
            s3_key="{{ task_instance.xcom_pull(key='status_csv_filename') }}",
            redshift_conn_id="redshift_default",
            aws_conn_id="aws_default",
            copy_options=["csv"],
        )

        fetch_status_data >> transform_status_data >> create_status_redshift_table >> transfer_status_data_to_redshift

    end = DummyOperator(task_id="end")

    start >> [weather_branch, status_branch] >> end
