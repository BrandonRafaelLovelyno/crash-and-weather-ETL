from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow.providers.mongo.hooks.mongo import MongoHook
import logging

logger = logging.getLogger(__name__)

def get_response(url):
  response = requests.get(url)
  if response.status_code != 200:
    raise Exception(f"Failed to retrieve data: {response.status_code}")
  return response.json()

def extract(ti, extraction_date):
    print("::group::Extraction Logging Details")

    crash_url = f"https://data.cityofnewyork.us/resource/h9gi-nx95.json?$$app_token=cEo4SXjWrqXOCeN34pQxbQ8ZN&$query=SELECT * WHERE crash_date BETWEEN '{extraction_date}' AND '{extraction_date}' ORDER BY crash_date DESC LIMIT 10000"
    crash_df = pd.DataFrame(get_response(crash_url))
    logger.info(f"Fetched {len(crash_df)} rows from crash data")

    weather_url = f"https://archive-api.open-meteo.com/v1/era5?latitude=40.730610&longitude=-73.935242&start_date={extraction_date}&end_date={extraction_date}&hourly=temperature_2m,precipitation"
    weather_df = pd.DataFrame(get_response(weather_url))
    logger.info(f"Fetched {len(weather_df)} rows from weather data")

    ti.xcom_push(key="crash_data", value=crash_df)
    print("crash_df head:", crash_df.head(5))
    logger.info("Pushed crash_data to XCom")

    ti.xcom_push(key="weather_data", value=weather_df)
    print("weather_df head:", weather_df.head(5))
    logger.info("Pushed weather_data to XCom")

    print("::endgroup::")

def transform(ti):
    print("::group::Transformation Logging Details")

    crash_df = ti.xcom_pull(key="crash_data", task_ids="extract_data")
    weather_df = ti.xcom_pull(key="weather_data", task_ids="extract_data")

    time_weather_df = pd.DataFrame({
        "time": pd.to_datetime(weather_df["hourly"]["time"]),
        "temperature": pd.to_numeric(weather_df["hourly"]["temperature_2m"]),
        "precipitation": pd.to_numeric(weather_df["hourly"]["precipitation"])
    })

    injured_mismatched_rows = crash_df[crash_df["number_of_persons_injured"].apply(pd.to_numeric) < crash_df[["number_of_pedestrians_injured", "number_of_cyclist_injured", "number_of_motorist_injured"]].apply(pd.to_numeric).sum(axis=1)]
    assert len(injured_mismatched_rows) == 0, "Mismatch found in the sum of injured persons."

    killed_mismatched_rows = crash_df[crash_df["number_of_persons_killed"].apply(pd.to_numeric) < crash_df[["number_of_pedestrians_killed", "number_of_cyclist_killed", "number_of_motorist_killed"]].apply(pd.to_numeric).sum(axis=1)]
    assert len(killed_mismatched_rows) == 0, "Mismatch found in the sum of killed persons."

    selected_columns = ["borough", "zip_code", "latitude", "longitude", "number_of_persons_injured", "number_of_persons_killed", "contributing_factor_vehicle_1", "contributing_factor_vehicle_2", "vehicle_type_code1", "vehicle_type_code2", "cross_street_name"]
    cleaned_crash_df = crash_df.loc[:, selected_columns]
    cleaned_crash_df["timestamp"] = pd.to_datetime(crash_df["crash_date"]) + pd.to_timedelta(crash_df["crash_time"] + ":00")

    numeric_columns = ["latitude", "longitude", "number_of_persons_injured", "number_of_persons_killed"]
    cleaned_crash_df[numeric_columns] = cleaned_crash_df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    sorted_crash_df = cleaned_crash_df.sort_values("timestamp")

    transformed_df = pd.merge_asof(
        sorted_crash_df,
        time_weather_df,
        left_on="timestamp",
        right_on="time",
        direction="backward"  # Ensures we match the latest 'time' <= 'timestamp'
    )

    #Fix Data Types
    transformed_df['zip_code'] = transformed_df['zip_code'].fillna(0).astype(int).astype(str).str.zfill(5).replace("00000", "Other")

    #Handle Inconsistent Data
    transformed_df = (transformed_df.fillna({"borough": "Other","contributing_factor_vehicle_1": "Other","contributing_factor_vehicle_2": "Other","vehicle_type_code1": "Other","vehicle_type_code2": "Other","cross_street_name": "Other","timestamp": "Other"}).replace(["Unspecified", "UNKNOWN", "NaN"], "Other"))
    
    #Cleaning Missing Value
    transformed_df = transformed_df.dropna(subset=["latitude","longitude"])

    ti.xcom_push(key="transformed_data", value=transformed_df)

    logger.info("Successfully transformed data")
    print("::endgroup::")

def load(ti):
    print("::group::Load Logging Details")

    data = ti.xcom_pull(key="transformed_data", task_ids="transform_data")

    hook = MongoHook(mongo_conn_id="mongo_crashdb")
    client = hook.get_conn()
    db = (client.data_engineering)
    crash_collection = db.crash_collection
    logger.info(f"Connected to MongoDB - {client.server_info()}")

    data_dict = data.to_dict(orient='records')
    crash_collection.insert_many(data_dict)    

    logger.info("Successfully load data to MongoDB")

    print("::endgroup::")

# Define DAG
with DAG(
    'Crash and Weather Data ETL',
    default_args={'retries': 1},
    description='An ETL pipeline for crash and weather data',
    schedule_interval='@daily',
    start_date=datetime(2024, 9, 20),
    catchup=True,
) as dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        op_kwargs={
            "extraction_date": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        },
    )
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform
    )
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load
    )

    extract_task >> transform_task >> load_task