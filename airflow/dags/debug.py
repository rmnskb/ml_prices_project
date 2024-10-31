from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
import sys
from pprint import pprint

default_args = {
    'owner': 'airflow'
    , 'start_date': datetime(2024, 9, 27)
    , 'retries': 1
    , 'retry_delay': timedelta(minutes=1)
}

with DAG(
    'debug'
    , default_args=default_args
    , schedule_interval='@daily'
):
    task_list = ['debug']

    debug = BashOperator(
        task_id='debug'
        # , bash_command='scrapy crawl estate_ids'
        # , cwd='../scrape/estates_scraping'
        # , cwd=(path + '/scrape/estates_scraping')
        , bash_command=''
        # , cwd=path
    )


