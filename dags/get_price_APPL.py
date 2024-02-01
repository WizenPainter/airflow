from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(dag_id="get_price_APPL",
        start_date=datetime(2024,1,31),
        schedule_interval="@weekly",
        catchup=False) as dag:

    @task
    def extract(symbol):
        return symbol

    @task
    def process(symbol):
        return symbol

    @task
    def store(symbol):
        return symbol

    store(process(extract(1234)))
