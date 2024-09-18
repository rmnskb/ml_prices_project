from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import psycopg2
import os

load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')


def get_db():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    return conn


def get_connection():
    connection_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    return connection_str


def init_db() -> None:
    with create_engine(get_connection()).connect() as conn:
        # Open and read the SQL file

        with open('init_db.sql', 'r') as sql_file:
            sql_script = text(sql_file.read())
            conn.execute(sql_script)
