import datetime
import logging
import sqlite3
import time
from http import HTTPStatus

import pytest
import requests
from airflow import DAG
from airflow.models import TaskInstance
from elasticsearch import Elasticsearch
from requests.auth import HTTPBasicAuth

from etl import sqlite_to_postgres
from etl.pg_to_es.extracters import IPEMExtracter, TargetExtracer
from etl.pg_to_es.loaders import Loader
from etl.pg_to_es.pipelines import MoviesETL
from etl.pg_to_es.transformers import PgToESTransformer
from etl.state import BaseUniqueStorage

LOGGER_NAME = "logs/etl.log"
logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(logging.FileHandler(LOGGER_NAME))

BATCH_SIZE = 256
TABLE_SQLITE_PG = {
    "person": (
        [
            ("full_name", "full_name"),
            ("birth_date", "birth_date"),
            ("id", "id"),
            ("created_at", "created"),
        ]
    ),
    "person_film_work": (
        [
            ("film_work_id", "film_work_id"),
            ("person_id", "person_id"),
            ("role", "role"),
            ("created_at", "created"),
            ("id", "id"),
        ]
    ),
    "film_work": (
        [
            ("title", "title"),
            ("description", "description"),
            ("type", "type"),
            ("creation_date", "creation_date"),
            ("certificate", "certificate"),
            ("file_path", "file_path"),
            ("rating", "rating"),
            ("id", "id"),
            ("created_at", "created"),
        ]
    ),
    "genre": [
        ("name", "name"),
        ("description", "description"),
        ("id", "id"),
        ("created_at", "created"),
    ],
    "genre_film_work": [
        ("film_work_id", "film_work_id"),
        ("genre_id", "genre_id"),
        ("created_at", "created"),
        ("id", "id"),
    ],
}


def relace_brackets(q: str) -> str:
    q = q.replace("'", "")
    q = q.replace("[", "")
    q = q.replace("]", "")
    return q


def compare_2_coursors(pg_curs, sq_curs):
    terminate = False
    while not terminate:
        fetched_pg = pg_curs.fetchmany(BATCH_SIZE)
        fetched_sq = sq_curs.fetchmany(BATCH_SIZE)

        # check if this is the end of the query result
        if (len(fetched_pg) < BATCH_SIZE) or (len(fetched_sq) < BATCH_SIZE):
            terminate = True
        # parse data to output type
        # todo save parse
        for d_pg, d_sq in zip(fetched_pg, fetched_sq):
            msg = "pg != sq\n{_d1}!={_d2}".format(
                _d1=d_pg,
                _d2=d_sq,
            )
            assert d_pg[0] == d_sq[0], msg


def test_table_names(pg_conn, sqlite_conn: sqlite3.Connection):

    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute(
            "".join(
                [
                    "SELECT table_name FROM information_schema.tables ",
                    "WHERE table_schema='content'",
                ]
            )
        )

        sq_lite_curs = sqlite_conn.cursor()
        sq_lite_curs.execute(
            "SELECT name FROM sqlite_master WHERE type='table';",
        )

        sq_tables = sq_lite_curs.fetchmany(len(TABLE_SQLITE_PG) + 1)
        pg_tables = pg_cursor.fetchmany(len(TABLE_SQLITE_PG) + 1)
        assert len(pg_tables) == len(TABLE_SQLITE_PG)
        assert len(sq_tables) == len(TABLE_SQLITE_PG)
        for t_name in pg_tables:
            assert t_name not in sq_tables


def test_etl_sqlite_to_pg(pg_conn, sqlite_conn: sqlite3.Connection):
    sqlite_to_postgres.main(
        pg_conn=pg_conn, sqlite_conn=sqlite_conn, batch_size=BATCH_SIZE
    )


def test_number_of_rows(pg_conn, sqlite_conn: sqlite3.Connection):
    pg_frmtbl = "SELECT COUNT(*) FROM content.{table};"
    sqlt_frmtbl = "SELECT COUNT(*) FROM {table};"

    with pg_conn.cursor() as pg_cursor:
        sq_lite_curs = sqlite_conn.cursor()
        for table_name in TABLE_SQLITE_PG.keys():
            pg_cursor.execute(pg_frmtbl.format(table=table_name))
            sq_lite_curs.execute(sqlt_frmtbl.format(table=table_name))
            rows_pg = pg_cursor.fetchone()[0]
            rows_sqlite = sq_lite_curs.fetchone()[0]
            err_msg = "{table}, pg: {r_pg}, sqlite: {r_sq}".format(
                r_pg=rows_pg, r_sq=rows_sqlite, table=table_name
            )
            assert rows_pg == rows_sqlite, err_msg
        sq_lite_curs.close()


def test_every_table_every_line(pg_conn, sqlite_conn: sqlite3.Connection):

    with pg_conn.cursor() as pg_cursor:
        sq_lite_curs = sqlite_conn.cursor()
        for table_name, fields in TABLE_SQLITE_PG.items():
            pg_flds = [fld[1] for fld in fields]
            sq_flds = [fld[0] for fld in fields]
            q = "SELECT {fld} FROM content.{tbl} ORDER BY id DESC;".format(
                fld=pg_flds,
                tbl=table_name,
            )
            q = relace_brackets(q)

            pg_cursor.execute(q)
            q = "SELECT {flds} FROM {tbl} ORDER BY id DESC;".format(
                flds=sq_flds,
                tbl=table_name,
            )

            q = relace_brackets(q)

            sq_lite_curs.execute(q)
            compare_2_coursors(pg_cursor, sq_lite_curs)


def test_enable_dag():
    resp = requests.patch(
        "http://airflow-webserver:8080/api/v1/dags/movies_etl_pg_to_es",
        json={
            "is_paused": False,
        },
        auth=HTTPBasicAuth("airflow", "airflow"),
    )

    assert resp.status_code == HTTPStatus.OK


def test_dag():
    resp = requests.post(
        "http://airflow-webserver:8080/api/v1/dags/movies_etl_pg_to_es/dagRuns",
        json={
            "dag_run_id": f"test_run_{datetime.datetime.now().strftime('%m/%d/%Y, %H:%M:%S')}",
        },
        auth=HTTPBasicAuth("airflow", "airflow"),
    )
    time.sleep(20)
    assert resp.json() == 1


def test_number_of_fw(es_conn: Elasticsearch):
    resp = es_conn.search(index="movies", query={"match_all": {}})

    assert resp["hits"]["total"]["value"] == 999


def test_nans(es_conn: Elasticsearch):
    resp = es_conn.search(index="movies", query={"query_string": {"query": "N\\A"}})

    assert resp["hits"]["total"]["value"] == 2


def test_search_camp(es_conn: Elasticsearch):
    resp = es_conn.search(
        index="movies",
        query={
            "multi_match": {
                "query": "camp",
                "fuzziness": "auto",
                "fields": [
                    "actors_names",
                    "writers_names",
                    "title",
                    "description",
                    "genre",
                ],
            }
        },
    )

    assert resp["hits"]["total"]["value"] == 24


def test_actor_query(es_conn: Elasticsearch):
    resp = es_conn.search(
        index="movies",
        query={
            "nested": {
                "path": "actors",
                "query": {"bool": {"must": [{"match": {"actors.name": "Greg Camp"}}]}},
            }
        },
    )

    assert resp["hits"]["total"]["value"] == 6


def test_find_filed_duplicates(es_conn: Elasticsearch):

    resp = es_conn.search(
        index="movies",
        query={"term": {"id": {"value": "68dfb5e2-7014-4738-a2da-c65bd41f5af5"}}},
    )
    assert resp["hits"]["total"]["value"] == 1
    assert resp["hits"]["hits"][0]["_source"]["writers_names"] == ["Lucien Hubbard"]


def test_one_writer(es_conn: Elasticsearch):
    resp = es_conn.search(
        index="movies",
        query={"term": {"id": {"value": "24eafcd7-1018-4951-9e17-583e2554ef0a"}}},
    )

    assert resp["hits"]["total"]["value"] == 1
    assert resp["hits"]["hits"][0]["_source"]["writers_names"] == ["Craig Hutchinson"]


def test_no_writer(es_conn: Elasticsearch):
    resp = es_conn.search(
        index="movies",
        query={"term": {"id": {"value": "479f20b0-58d1-4f16-8944-9b82f5b1f22a"}}},
    )

    assert resp["hits"]["total"]["value"] == 1
    assert resp["hits"]["hits"][0]["_source"]["directors_names"] == []
