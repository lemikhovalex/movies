import datetime
import time
import typing as tp
import urllib.parse
from abc import ABC
from http import HTTPStatus

import pytest
import requests
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from requests.auth import HTTPBasicAuth

from etl.config import CONFIG
from etl.pg_to_es.extracters import IPEMExtracter, TargetExtracer
from etl.pg_to_es.loaders import Loader
from etl.pg_to_es.pipelines import MoviesETL
from etl.pg_to_es.spark import (
    ElasticLoader,
    FilmWorkTransformer,
    PostgreExtractor,
    get_postgres_es_session,
)
from etl.pg_to_es.transformers import PgToESTransformer
from etl.state import BaseUniqueStorage


@pytest.fixture(scope="class")
def spark_session() -> tp.Generator[SparkSession, None, None]:
    session = get_postgres_es_session(
        master_host=CONFIG.spark_master_host,
        master_port=CONFIG.spark_master_port,
        app_name="etl_test",
    )

    yield session

    session.stop()


class BaseTests(ABC):
    def test_number_of_fw(self, es_conn: Elasticsearch):
        resp = es_conn.search(index="movies", query={"match_all": {}})

        assert resp["hits"]["total"]["value"] == 999

    def test_nans(self, es_conn: Elasticsearch):
        resp = es_conn.search(index="movies", query={"query_string": {"query": "N\\A"}})

        assert resp["hits"]["total"]["value"] == 2

    def test_search_camp(self, es_conn: Elasticsearch):
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

    def test_actor_query(self, es_conn: Elasticsearch):
        resp = es_conn.search(
            index="movies",
            query={
                "nested": {
                    "path": "actors",
                    "query": {
                        "bool": {"must": [{"match": {"actors.name": "Greg Camp"}}]}
                    },
                }
            },
        )

        assert resp["hits"]["total"]["value"] == 6

    def test_find_filed_duplicates(self, es_conn: Elasticsearch):

        resp = es_conn.search(
            index="movies",
            query={"term": {"id": {"value": "68dfb5e2-7014-4738-a2da-c65bd41f5af5"}}},
        )
        assert resp["hits"]["total"]["value"] == 1
        assert resp["hits"]["hits"][0]["_source"]["writers_names"] == ["Lucien Hubbard"]

    def test_one_writer(self, es_conn: Elasticsearch):
        resp = es_conn.search(
            index="movies",
            query={"term": {"id": {"value": "24eafcd7-1018-4951-9e17-583e2554ef0a"}}},
        )

        assert resp["hits"]["total"]["value"] == 1
        assert resp["hits"]["hits"][0]["_source"]["writers_names"] == [
            "Craig Hutchinson"
        ]

    def test_no_writer(self, es_conn: Elasticsearch):
        resp = es_conn.search(
            index="movies",
            query={"term": {"id": {"value": "479f20b0-58d1-4f16-8944-9b82f5b1f22a"}}},
        )

        assert resp["hits"]["total"]["value"] == 1
        assert resp["hits"]["hits"][0]["_source"]["directors_names"] == []


class FillESPlain(ABC):
    def test_pg(self, request):
        request.getfixturevalue("pg_conn")

    @pytest.mark.parametrize(
        "extracter,queue",
        [
            (
                "person_extracter",
                "person_queue",
            ),
            (
                "genre_extracter",
                "genre_queue",
            ),
            (
                "fw_extracter",
                "fw_queue",
            ),
        ],
    )
    def test_fill_base_q(
        self, extracter: IPEMExtracter, queue: BaseUniqueStorage, request
    ):
        extracter = request.getfixturevalue(extracter)
        queue = request.getfixturevalue(queue)
        baes_prod = extracter.produce_base()

        for base_batch in baes_prod:
            queue.update(base_batch)

    @pytest.mark.parametrize(
        "extracter,queue,batch_size",
        [
            (
                "person_extracter",
                "person_queue",
                5,
            ),
            (
                "genre_extracter",
                "genre_queue",
                1,
            ),
        ],
    )
    def test_extraction_to_q(
        self,
        fw_queue: BaseUniqueStorage,
        extracter: IPEMExtracter,
        queue: BaseUniqueStorage,
        batch_size: int,
        request,
    ):
        extracter = request.getfixturevalue(extracter)
        queue = request.getfixturevalue(queue)

        base_prod = queue.get_iterator(batch_size)

        for base_batch in base_prod:
            target_ids = extracter.get_target_ids(base_batch)
            fw_queue.update(target_ids)

    def test_q_size(self, fw_queue: BaseUniqueStorage):
        assert len(fw_queue) == 999

    def test_extracted_from_q(self, fw_queue: BaseUniqueStorage, pg_conn):

        with pg_conn.cursor() as pg_cursor:
            pg_cursor.execute("SELECT COUNT(*) FROM content.film_work;")
            rows_pg = pg_cursor.fetchone()[0]
        assert len(fw_queue) == rows_pg

    def test_fill_es_from_q(self, pg_conn, fw_queue: BaseUniqueStorage, es_factory):
        q_extracter = TargetExtracer(
            pg_connection=pg_conn, u_storage=fw_queue, batch_size=256
        )
        transformer = PgToESTransformer()
        loader = Loader(index="movies", es_factory=es_factory, debug=True)
        etl = MoviesETL(
            extracter=q_extracter,
            transformer=transformer,
            loader=loader,
        )
        etl.run()


class FillESAF(ABC):
    dag_run_id = f"test_run_{datetime.datetime.now().strftime('%d%m%Y%H%M%S')}"
    air_flow_webserver = "http://airflow-webserver:8080/airflow/"

    def test_pg(self, request):
        request.getfixturevalue("pg_conn")

    def test_enable_dag(self):
        resp = requests.patch(
            urllib.parse.urljoin(
                self.air_flow_webserver, "api/v1/dags/movies_etl_pg_to_es"
            ),
            json={
                "is_paused": False,
            },
            auth=HTTPBasicAuth("airflow", "airflow"),
        )

        assert resp.status_code == HTTPStatus.OK

    def test_dag(self, es_factory):
        resp = requests.post(
            urllib.parse.urljoin(
                self.air_flow_webserver, "api/v1/dags/movies_etl_pg_to_es/dagRuns"
            ),
            json={
                "dag_run_id": self.dag_run_id,
            },
            auth=HTTPBasicAuth("airflow", "airflow"),
        )
        assert resp.status_code == HTTPStatus.OK

    def test_dag_completion(self):
        s = time.time()
        is_success: bool = False
        status = "no_q"
        while ((time.time() - s) < 60) and (not is_success):
            resp = requests.get(
                urllib.parse.urljoin(
                    self.air_flow_webserver,
                    f"api/v1/dags/movies_etl_pg_to_es/dagRuns/{self.dag_run_id}",
                ),
                auth=HTTPBasicAuth("airflow", "airflow"),
            )
            assert resp.status_code == HTTPStatus.OK, self.dag_run_id + str(resp.json())
            status = resp.json()["state"]
            is_success = status == "success"
            assert status != "failed"
            time.sleep(1)

        assert is_success, status


class FillESSpark(ABC):
    def test_spark_etl(self, request, spark_session: SparkSession):
        request.getfixturevalue("pg_conn")
        request.getfixturevalue("es_factory")
        extracter = PostgreExtractor(
            session=spark_session,
            db_host=CONFIG.db_host,
            db_port=CONFIG.db_port,
            db_user=CONFIG.db_user,
            db_password=CONFIG.db_password,
            db_name=CONFIG.db_name,
        )
        query = """
            select
                fw.id as id,
                fw.rating  as imdb_rating,
                json_agg(DISTINCT g.name) as genres_names,
                fw.title as title,
                fw.description as description,
                coalesce(
                    json_agg(DISTINCT p.full_name)
                    FILTER (WHERE p_fw.role = 'actor' and p.id is not NULL),
                    '[]'
                ) actors_names,
                coalesce(
                    json_agg(DISTINCT p.full_name)
                    FILTER (WHERE p_fw.role = 'director' and p.id is not NULL),
                    '[]'
                ) directors_names,
                coalesce(
                    json_agg(DISTINCT p.full_name)
                    FILTER (WHERE p_fw.role = 'writer' and p.id is not NULL),
                    '[]'
                ) writers_names,
                coalesce (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', p.id,
                        'name', p.full_name
                    )
                ) FILTER (WHERE p_fw.role = 'actor' and p.id is not NULL),
                '[]'
            ) as actors,
                coalesce (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', p.id,
                        'name', p.full_name
                    )
                ) FILTER (WHERE p_fw.role = 'writer' and p.id is not NULL),
                '[]'
            ) as writers,
                coalesce (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', p.id,
                        'name', p.full_name
                    )
                ) FILTER (WHERE p_fw.role = 'director' and p.id is not NULL),
                '[]'
            ) as directors,
                coalesce (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', g.id,
                        'name', g.name
                    )
                ) FILTER (WHERE g.id is not NULL),
                '[]'
            ) as genres
            from content.film_work as fw
            full join content.genre_film_work as g_fw on g_fw.film_work_id = fw.id
            full join content.genre as g on g.id = g_fw.genre_id
            full join content.person_film_work as p_fw on p_fw.film_work_id = fw.id
            full join content.person as p on p_fw.person_id = p.id
            group by fw.id
        """

        df = extracter.extract(query=query)

        transformer = FilmWorkTransformer()
        df = transformer.transform(df=df)

        loader = ElasticLoader(
            es_host=CONFIG.es_host, es_port=CONFIG.es_port, index_name="movies"
        )
        loader.load(df)


class TestPlainPgToES(BaseTests, FillESPlain):
    ...


class TestAFPgToES(BaseTests, FillESAF):
    ...


class TestSparkETL(BaseTests, FillESSpark):
    ...
