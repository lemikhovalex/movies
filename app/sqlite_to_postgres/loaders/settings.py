import os

from dotenv import load_dotenv


def get_dsl(path_to_dotenv: str):
    load_dotenv(path_to_dotenv)
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
    }
    return dsl
