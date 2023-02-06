import sqlite3
from typing import Callable, Generator

import psycopg2
import pytest
import redis
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor
from src.utils.pg_utils import clean_pg

from etl import constants, sqlite_to_postgres
from etl.config import CONFIG
from etl.pg_to_es.extracters import (
    FilmworkExtracter,
    GenreExtracter,
    IExtracter,
    PersonExtracter,
)
from etl.state import BaseUniqueStorage, GenericFileStorage, RedisQueue, State

BATCH_SIZE = 256


@pytest.fixture(scope="class")
def pg_conn(sqlite_conn: sqlite3.Connection):

    dsl = {
        "dbname": CONFIG.db_name,
        "user": CONFIG.db_user,
        "password": CONFIG.db_password,
        "host": CONFIG.db_host,
        "port": CONFIG.db_port,
    }
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        clean_pg(pg_conn)
        sqlite_to_postgres.main(
            pg_conn=pg_conn, sqlite_conn=sqlite_conn, batch_size=BATCH_SIZE
        )
        yield pg_conn
        clean_pg(pg_conn)


@pytest.fixture(scope="class")
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


@pytest.fixture(scope="class")
def redis_conn():
    r = redis.Redis(
        port=CONFIG.redis_port,
        host=CONFIG.redis_host,
        decode_responses=True,
    )
    r.flushall()

    yield r

    r.flushall()


@pytest.fixture(scope="class")
def es_conn(es_factory: Callable[[], Elasticsearch]):
    return es_factory()


@pytest.fixture(scope="class")
def fw_queue(redis_conn: redis.Redis) -> BaseUniqueStorage:
    return RedisQueue(q_name="fw", conn=redis_conn)


@pytest.fixture(scope="class")
def person_queue(redis_conn: redis.Redis) -> BaseUniqueStorage:
    return RedisQueue(q_name="p", conn=redis_conn)


@pytest.fixture(scope="class")
def genre_queue(redis_conn: redis.Redis) -> Generator[BaseUniqueStorage, None, None]:
    yield RedisQueue(q_name="g", conn=redis_conn)


@pytest.fixture(scope="class")
def person_extracter(pg_conn) -> IExtracter:
    return PersonExtracter(
        pg_connection=pg_conn,
        state=State(GenericFileStorage()),
        batch_size=50,
    )


@pytest.fixture(scope="class")
def genre_extracter(pg_conn) -> IExtracter:
    return GenreExtracter(
        pg_connection=pg_conn,
        state=State(GenericFileStorage()),
        batch_size=2,
    )


@pytest.fixture(scope="class")
def fw_extracter(pg_conn) -> IExtracter:
    return FilmworkExtracter(
        pg_connection=pg_conn,
        state=State(GenericFileStorage()),
        batch_size=10,
    )
