from collections import OrderedDict

from pyspark.sql import SparkSession

SPARK_DIR = "/opt/workspace"
spark_jars = [
    "org.apache.hadoop:hadoop-aws:3.2.1",
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
    ) as directors
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
# dbDataFrame.show(5)
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
# The following parameters can control the amount and number of data quantities and
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
