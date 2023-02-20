from pyspark.sql import SparkSession

SPARK_DIR = "/opt/workspace"

spark_session = (
    SparkSession.builder.appName("Python Spark SQL basic example")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.driver.extraClassPath", f"{SPARK_DIR}/postgresql-42.5.1.jar")
    .getOrCreate()
)
print(f"{SPARK_DIR}/postgresql-42.5.1.jar")

dbDataFrame = (
    spark_session.read.format("jdbc")
    .option("url", "jdbc:postgresql://admin_db:15432/movies")
    .option("dbtable", "content.genre")
    .option("user", "app")
    .option("password", "app")
    .option("driver", "org.postgresql.Driver")
)
print(2)
dbDataFrame = dbDataFrame.load()
print(3)
dbDataFrame.show(5)
print(3)
