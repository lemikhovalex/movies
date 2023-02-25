import datetime
import time
import urllib.parse
from abc import ABC
from collections import OrderedDict
from http import HTTPStatus

import pytest
import requests
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import ArrayType, MapType, StringType
from requests.auth import HTTPBasicAuth

from etl.pg_to_es.extracters import IPEMExtracter, TargetExtracer
from etl.pg_to_es.loaders import Loader
from etl.pg_to_es.pipelines import MoviesETL
from etl.pg_to_es.transformers import PgToESTransformer
from etl.state import BaseUniqueStorage


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
        # time.sleep(20)
        assert resp.status_code == HTTPStatus.OK

    def test_dag_completion(self):
        s = time.time()
        is_success: bool = False
        status = "no_q"
        while ((time.time() - s) < 30) and (not is_success):
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
    def test_spark_etl(self, request):
        request.getfixturevalue("pg_conn")
        request.getfixturevalue("es_factory")
        spark_jars = [
            # "org.apache.hadoop:hadoop-aws:3.3.1",
            "org.postgresql:postgresql:42.2.10",
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.6.2",
        ]
        spark_session = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName("Python Spark SQL basic example")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.jars.packages", ",".join(spark_jars))
            .getOrCreate()
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
                ) FILTER (WHERE p_fw.role = 'director' and p.id is not NULL),
                '[]'
            ) as genres
            from content.film_work as fw
            join content.genre_film_work as g_fw on g_fw.film_work_id = fw.id
            join content.genre as g on g.id = g_fw.genre_id
            join content.person_film_work as p_fw on p_fw.film_work_id = fw.id
            join content.person as p on p_fw.person_id = p.id
            group by fw.id
        """

        dbDataFrame = (
            spark_session.read.format("jdbc")
            .option("url", "jdbc:postgresql://admin_db:15432/movies")
            .option("driver", "org.postgresql.Driver")
            .option("user", "app")
            .option("password", "app")
            .option(
                "query",
                query,
            )
        )

        print(2)
        dbDataFrame = dbDataFrame.load()
        print(3)
        for c in ["actors", "writers", "genres", "directors"]:
            dbDataFrame = dbDataFrame.withColumn(
                c,
                from_json(
                    getattr(dbDataFrame, c),
                    MapType(
                        StringType(),
                        StringType(),
                    ),
                ),
            )
        for c in ["genres_names", "directors_names", "actors_names", "writers_names"]:
            dbDataFrame = dbDataFrame.withColumn(
                c,
                from_json(
                    getattr(dbDataFrame, c),
                    ArrayType(StringType()),
                ),
            )
        dbDataFrame.show(5)
        print(4)

        # Write the result into ES
        options = OrderedDict()
        options["es.nodes"] = "elasticsearch"
        options["es.port"] = "9200"
        options["es.Resource"] = "movies"
        # Connect the timeout time setting of the ES. Default 1M
        options["es.http.timeout"] = "10000m"
        options["es.nodes.wan.only"] = "true"
        # Default retry 3 times, if the negative value is an unlimited retry (careful)
        options["es.batch.write.retry.count"] = "15"
        # Default Retry Waiting Time is 10s
        options["es.batch.write.retry.wait"] = "60"
        # The following parameters can control the amount and number
        # of data quantities and
        # numbers written in a single batch (two choices)
        options["es.batch.size.bytes"] = "2mb"
        options["es.batch.size.entries"] = "128"

        # # elasticsearch-spark-20_2.10-7.17.9.jar
        _ = (
            dbDataFrame.write.format("org.elasticsearch.spark.sql")
            .options(**options)
            .mode("overwrite")
            .save()
        )


class TestPlainPgToES(BaseTests, FillESPlain):
    ...


class TestAFPgToES(BaseTests, FillESAF):
    ...


class TestSparkETL(BaseTests, FillESSpark):
    ...
