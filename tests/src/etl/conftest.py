import psycopg2
import pytest
from psycopg2.extras import DictCursor

from etl.config import CONFIG
from tests.src.utils.pg_utils import clean_pg


@pytest.fixture(scope="session")
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
