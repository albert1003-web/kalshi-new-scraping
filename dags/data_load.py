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
        return [("kalshi_data/events_keywords.csv", "EVENTS_KEYWORDS"), ("kalshi_data/kalshi_events.csv", "KALSHI_EVENTS")]
    
    @task 
    def get_nyt_data():
        events = NYT_scraping.get_recent_articles()
        NYT_scraping.save_to_csv(events)
        # load keywords.csv only once here since it has the all entries now
        return [("nyt_data/nyt_articles.csv", "NY_TIMES_ARTICLES"), ("nyt_data/articles_keywords.csv", "ARTICLES_KEYWORDS"), ("keywords.csv", "KEYWORDS")]


    @task
    def load_file_to_stage(file_data):
        # load CSV file into stage
        filename, table = file_data
        base = os.path.basename(filename)
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        path = f"/usr/local/airflow/include/data/{filename}"
        put = f"PUT file://{path} @PUBLIC.KALSHI_STAGE OVERWRITE = TRUE"
        copy_into = f"""COPY INTO {table} FROM @PUBLIC.KALSHI_STAGE/{base}.gz FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"') FORCE = TRUE"""
        remove = f"REMOVE @PUBLIC.KALSHI_STAGE/{base}.gz"
        print(f"Loading {filename}...")
        hook.run(put)
        hook.run(copy_into)
        hook.run(remove)
        return filename

    @task
    def merge_file_info(kalshi, nyt):
        return kalshi + nyt

    @task
    def truncate_tables():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        tables = ["EVENTS_KEYWORDS", "KALSHI_EVENTS", "KEYWORDS", "NY_TIMES_ARTICLES", "ARTICLES_KEYWORDS"]
        for table in tables:
            hook.run(f"TRUNCATE TABLE $[SNOWFALE_DATABASE].PUBLIC.{table}")

    @task(outlets=[raw_kalshi_dataset])
    def update_raw_kalshi_dataset():
        print("Raw Kalshi data was updated.")

    truncated = truncate_tables()
    kalshi_file_info = get_kalshi_data()
    nyt_file_info = get_nyt_data()
    truncated >> kalshi_file_info
    truncated >> nyt_file_info
    file_info = merge_file_info(kalshi_file_info, nyt_file_info)
    loaded_files = load_file_to_stage.expand(file_data=file_info)
    loaded_files >> update_raw_kalshi_dataset()
    
