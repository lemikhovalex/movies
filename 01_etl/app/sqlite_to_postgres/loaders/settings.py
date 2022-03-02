import os

from dotenv import load_dotenv


def get_dsl(path_to_dotenv: str):
    load_dotenv(path_to_dotenv)
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": "127.0.0.1",
        "port": 5432,
    }
    return dsl
