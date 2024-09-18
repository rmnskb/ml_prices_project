from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'airflow'
    , 'start_date': datetime(2024, 9, 18)
    , 'retries': 1
    , 'retry_delay': timedelta(minutes=10)
}

with DAG(
    'scrape_clean_featurise'
    , default_args=default_args
    , description='Scrape the data, clean it and create features, then post it to DB'
    , schedule_interval='@daily'
):
    task_list = ['scrape_ids', 'scrape_features', 'get_gis', 'clean_and_featurise']

    scape_ids = BashOperator(
        task_id='scrape_ids'
        , bash_command='scrapy crawl estate_ids'
        , cwd='../scrape/estates_scraping'
    )

