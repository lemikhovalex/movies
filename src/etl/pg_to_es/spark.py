from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import ArrayType, MapType, StringType


def get_postgres_es_session(
    master_host: str, master_port: int, app_name: str
) -> SparkSession:
    spark_jars = [
        "org.postgresql:postgresql:42.2.10",
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.6.2",
    ]
    spark_session = (
        SparkSession.builder.master(f"spark://{master_host}:{master_port}")
        .appName(app_name)
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.jars.packages", ",".join(spark_jars))
        .getOrCreate()
    )
    return spark_session


class PostgreExtractor:
    def __init__(
        self,
        session: SparkSession,
        db_host: str,
        db_port: int,
        db_user: str,
        db_password: str,
        db_name: str,
    ):
        self.session = session
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name

    def extract(self, query: str) -> DataFrame:

        db_f_reader = (
            self.session.read.format("jdbc")
            .option(
                "url",
                "jdbc:postgresql://{host}:{port}/{name}".format(
                    host=self.db_host, port=self.db_port, name=self.db_name
                ),
            )
            .option("driver", "org.postgresql.Driver")
            .option("user", self.db_user)
            .option("password", self.db_password)
            .option(
                "query",
                query,
            )
        )
        out = db_f_reader.load()
        return out


class FilmWorkTransformer:
    def transform(self, df: DataFrame) -> DataFrame:
        for c in ["actors", "writers", "genres", "directors"]:
            df = df.withColumn(
                c,
                from_json(
                    getattr(df, c),
                    MapType(
                        StringType(),
                        StringType(),
                    ),
                ),
            )
        for c in ["genres_names", "directors_names", "actors_names", "writers_names"]:
            df = df.withColumn(
                c,
                from_json(
                    getattr(df, c),
                    ArrayType(StringType()),
                ),
            )
        return df


class ElasticLoader:
    def __init__(
        self, es_host: str, es_port: int, config: dict[str, str] | None = None
    ):
        self.es_host = es_host
        self.es_port = es_port
        if config is None:
            self.config = {
                "es.Resource": "movies",
                "es.http.timeout": "10000m",
                "es.nodes.wan.only": "true",
                "es.batch.write.retry.count": "15",
                "es.batch.write.retry.wait": "60",
                "es.batch.size.bytes": "1mb",
                "es.batch.size.entries": "64",
                "es.batch.write.refresh": "true",
            }
        else:
            self.config = config

    def load(self, df: DataFrame):
        options = {}

        options["es.nodes"] = self.es_host
        options["es.port"] = str(self.es_port)
        options.update(self.config)

        # # elasticsearch-spark-20_2.10-7.17.9.jar
        _ = (
            df.write.format("org.elasticsearch.spark.sql")
            .options(**options)
            .mode("append")
            .save()
        )
