from dotenv import load_dotenv
from psycopg2 import sql
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
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
    with get_db() as db:
        # Open and read the SQL file
        cursor = db.cursor()
        with open('init_db.sql', 'r') as sql_file:
            sql_script = sql_file.read()

        # Execute the SQL script
        cursor.execute(sql.SQL(sql_script))

        # Commit the changes
        db.commit()


def check_ids() -> list[int]:
    with get_db() as db:
        cursor = db.cursor()
        cursor.execute(
            """
                SELECT id
                FROM postgres.public.flats
                WHERE checked_flg IS FALSE
                ORDER BY 
                    scrape_dt DESC
                    , id 
                LIMIT 1000
            """
        )
        ids = [row[0] for row in cursor.fetchall()]

    return ids


if __name__ == "__main__":
    check_ids()
