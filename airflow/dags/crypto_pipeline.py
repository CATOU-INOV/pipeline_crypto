from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 10, "page": 1}
    response = requests.get(url, params=params)
    data = response.json()
    print(json.dumps(data[:3], indent=2))  # juste pour tester

with DAG(
    'crypto_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',  # toutes les heures
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_crypto_data',
        python_callable=fetch_crypto_data,
    )
