from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.decorators import task, dag
from dotenv import load_dotenv
import os

path = Variable.get('path')

# scrapy_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scrape/estates_scraping'))

default_args = {
    'owner': 'bogdanr'
    , 'start_date': datetime(2024, 9, 18)
    , 'retries': 1
    , 'retry_delay': timedelta(minutes=1)
}


def id_spider():
    from scrape.estates_scraping.spiders.estate_id_spider import IDSpider


# @dag(
#     schedule='@daily'
#     , default_args=default_args
#     , description='Scrape the data, clean it and create features, then post it to DB'
#     # , dag_id='scrape_clean_featurise'
# )
# def scrape_clean_featurise():
#     @task(task_id='scrape_ids')
#     def scrape_ids():
#         from scrape.estates_scraping.spiders.estate_id_spider import IDSpider
#
#     scrape_general_data = scrape_ids()


with DAG(
    'scrape_clean_featurise'
    , default_args=default_args
    , description='Scrape the data, clean it and create features, then post it to DB'
    , schedule_interval='@daily'
):
    task_list = ['scrape_ids', 'scrape_features', 'get_gis', 'clean_and_featurise']

    # scape_ids = BashOperator(
    #     task_id='scrape_ids'
    #     # , bash_command=f'cd {path}scrape/ && scrapy crawl estate_ids'
    #     , bash_command='ls'
    #     # , cwd='../scrape/estates_scraping'
    #     # , cwd=(path + '/scrape/estates_scraping')
    #     # , bash_command='pwd'
    #     # , cwd=path
    # )

    scrape_ids = PythonOperator(
        task_id='scrape_ids'
        , python_callable=id_spider
    )
