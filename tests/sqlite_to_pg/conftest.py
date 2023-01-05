import os
import sqlite3
from typing import Generator

import psycopg2
import pytest
from psycopg2.extras import DictCursor


@pytest.fixture(scope="session")
def sqlite_conn() -> Generator[sqlite3.Connection, None, None]:
    with sqlite3.connect('db.sqlite') as sqlite_conn:
        yield sqlite_conn


@pytest.fixture(scope="session")
def pg_conn():

    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
    }
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        yield pg_conn

        with pg_conn.cursor() as cursor:
            cursor.execute("TRUNCATE content.person_film_work CASCADE;")
            pg_conn.commit()
            cursor.execute("TRUNCATE content.genre_film_work CASCADE;")
            pg_conn.commit()
            cursor.execute("TRUNCATE content.genre CASCADE;")
            pg_conn.commit()
            cursor.execute("TRUNCATE content.person CASCADE;")
            pg_conn.commit()
            cursor.execute("TRUNCATE content.film_work CASCADE;")
            pg_conn.commit()