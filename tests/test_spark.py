from pyspark.sql import SparkSession

SPARK_DIR = "/app_data/spark"


def get_spark_builder(spark_host: str, spark_port: int, app_name: str):
    return SparkSession.builder.appName(app_name).master(
        f"spark://{spark_host}:{spark_port}"
    )


spark_session = get_spark_builder(spark_host="spark", spark_port=7077, app_name="test")
spark_session = spark_session.config(
    "spark.jars", "{}/drivers/sqlite-jdbc-3.34.0.jar".format(SPARK_DIR)
).config("spark.driver.extraClassPath", "{}/sqlite-jdbc-3.34.0.jar".format(SPARK_DIR))
print(1)
spark_session = spark_session.getOrCreate()
print(2)
driver = "org.sqlite.JDBC"
path = "etl/db.sqlite"
url = "jdbc:sqlite:" + path
tablename = "film_work"
print(3)
dbDataFrame = (
    spark_session.read.format("jdbc")
    .option("url", url)
    .option("dbtable", tablename)
    .option("driver", driver)
    .load()
)
print(4)
dbDataFrame.show()
print(5)
