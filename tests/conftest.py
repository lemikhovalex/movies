import sqlite3
from typing import Callable, Generator

import psycopg2
import pytest
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor

from etl.pg_to_es.base import IExtracter
from etl.pg_to_es.extracters import FilmworkExtracter, GenreExtracter, PersonExtracter
from etl.state import BaseUniqueStorage, GenericFileStorage, GenericQueue, State
from tests import constants
from tests.config import CONFIG


def clean_pg(pg_conn):
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


@pytest.fixture(scope="session")
def sqlite_conn() -> Generator[sqlite3.Connection, None, None]:
    with sqlite3.connect("db.sqlite") as sqlite_conn:
        yield sqlite_conn


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


@pytest.fixture(scope="session")
def es_factory() -> Generator[Callable[[], Elasticsearch], None, None]:
    url = f"http://{CONFIG.es_host}:{CONFIG.es_port}"
    es = Elasticsearch(url)
    indecies = ["genres", "persons", "movies"]
    for idx in indecies:
        es.indices.create(
            index=idx,
            settings=constants.es_settings,
            mappings=getattr(constants, f"es_mappings_{idx}"),
        )
    es.close()
    del es

    def inner() -> Elasticsearch:
        return Elasticsearch(url)

    yield inner

    es = Elasticsearch(url)
    for idx in indecies:
        es.indices.delete(index=idx)

    es.close()


@pytest.fixture(scope="session")
def es_conn(es_factory: Callable[[], Elasticsearch]):
    return es_factory()


@pytest.fixture(scope="session")
def fw_queue() -> BaseUniqueStorage:
    return GenericQueue()


@pytest.fixture(scope="session")
def person_extracter(pg_conn) -> IExtracter:
    return PersonExtracter(
        pg_connection=pg_conn,
        state=State(GenericFileStorage()),
        batch_size=50,
    )


@pytest.fixture(scope="session")
def genre_extracter(pg_conn) -> IExtracter:
    return GenreExtracter(
        pg_connection=pg_conn,
        state=State(GenericFileStorage()),
        batch_size=2,
    )


@pytest.fixture(scope="session")
def fw_extracter(pg_conn) -> IExtracter:
    return FilmworkExtracter(
        pg_connection=pg_conn,
        state=State(GenericFileStorage()),
        batch_size=10,
    )
