import sqlite3
from typing import Generator

import psycopg2
import pytest
from psycopg2.extras import DictCursor
from src.utils.pg_utils import clean_pg

from etl.config import CONFIG


@pytest.fixture(scope="session")
def sqlite_conn() -> Generator[sqlite3.Connection, None, None]:
    with sqlite3.connect("tests/etl/db.sqlite") as sqlite_conn:
        yield sqlite_conn


@pytest.fixture(scope="module")
def pg_conn():

    dsl = {
        "dbname": CONFIG.db_name,
        "user": CONFIG.db_user,
        "password": CONFIG.db_password,
        "host": CONFIG.db_host,
        "port": CONFIG.db_port,
    }
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        clean_pg(pg_conn)
        yield pg_conn
        clean_pg(pg_conn)
