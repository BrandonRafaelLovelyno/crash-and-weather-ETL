from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

def get_response(url):
  response = requests.get(url)
  if response.status_code != 200:
    raise Exception(f"Failed to retrieve data: {response.status_code}")
  return response.json()

def extract(ti, extraction_date):
    crash_url = f"https://data.cityofnewyork.us/resource/h9gi-nx95.json?$$app_token=cEo4SXjWrqXOCeN34pQxbQ8ZN&$query=SELECT * WHERE crash_date BETWEEN '{extraction_date}' AND '{extraction_date}' ORDER BY crash_date DESC LIMIT 10000"
    crash_df = pd.DataFrame(get_response(crash_url))

    weather_url = f"https://archive-api.open-meteo.com/v1/era5?latitude=40.730610&longitude=-73.935242&start_date={extraction_date}&end_date={extraction_date}&hourly=temperature_2m,precipitation"
    weather_df = pd.DataFrame(get_response(weather_url))

    ti.xcom_push(key="crash_data", value=crash_df)
    ti.xcom_push(key="weather_data", value=weather_df)

def transform(ti):
    crash_df = ti.xcom_pull(key="crash_data", task_ids="extract")
    weather_df = ti.xcom_pull(key="weather_data", task_ids="extract")
    print("Extract Task:")
    print("crash_df", crash_df.head())
    print("weather_df", weather_df.head())

def load():
    print("Loading data...")

# Define DAG
with DAG(
    'testing_etl_pipeline',
    default_args={'retries': 1},
    description='A simple ETL pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
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
    # load_task = PythonOperator(task_id='load', python_callable=load)

    extract_task >> transform_task