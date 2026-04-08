from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from datetime import datetime, timedelta
from scripts import kalshi_scraping, NYT_scraping
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os

raw_kalshi_dataset = Dataset("snowflake://kalshi/raw_kalshi")

with DAG(
    dag_id='load_csv',
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False
) as load_csv:
    
    @task
    def get_kalshi_data():
        events = kalshi_scraping.get_all_kalshi_events()
        kalshi_scraping.save_to_csv(events)
        return [("kalshi_data/events_keywords.csv", "EVENTS_KEYWORDS"), ("kalshi_data/kalshi_events.csv", "KALSHI_EVENTS"), ("keywords.csv", "KEYWORDS")]

    @task
    def load_file_to_stage(file_data):
        # load CSV file into stage
        filename, table = file_data
        base = os.path.basename(filename)
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        path = f"/usr/local/airflow/include/data/{filename}"
        put = f"PUT file://{path} @PUBLIC.KALSHI_STAGE"
        copy_into = f"""COPY INTO {table} FROM @PUBLIC.KALSHI_STAGE/{base}.gz FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')"""
        print(f"Loading {filename}...")
        hook.run(put)
        hook.run(copy_into)
        return filename

    @task(outlets=[raw_kalshi_dataset])
    def update_raw_kalshi_dataset():
        print("Raw Kalshi data was updated.")

    file_info = get_kalshi_data()
    loaded_files = load_file_to_stage.expand(file_data=file_info)
    loaded_files >> update_raw_kalshi_dataset()
    
