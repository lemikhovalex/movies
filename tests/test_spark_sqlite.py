from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SPARK_DIR = "/opt/workspace"

spark_session = (
    SparkSession.builder.appName("Python Spark SQL basic example")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.driver.extraClassPath", f"{SPARK_DIR}/sqlite-jdbc-3.34.0.jar")
    .getOrCreate()
)
print(f"{SPARK_DIR}/sqlite-jdbc-3.34.0.jar")

dbDataFrame = (
    spark_session.read.format("jdbc")
    .option("url", "jdbc:sqlite:etl/db.sqlite")
    .option("driver", "org.sqlite.JDBC")
    .option("customSchema", "id INT, co_list STRING, last_page INT, saved INT")
    .option(
        "query",
        """
            SELECT
                id,
                name,
                description,
                CAST(
                    strftime(
                        '%s',
                        DATETIME(
                            SUBSTR(g.created_at,0, 27),
                            SUBSTR(g.created_at,27, 31) || ' hours'
                        )
                    ) AS INT
                ) as created_at,
                CAST(
                    strftime(
                        '%s',
                        DATETIME(
                            SUBSTR(g.updated_at ,0, 27),
                            SUBSTR(g.updated_at,27, 31) || ' hours'
                        )
                    ) AS INT
                ) as updated_at
            from genre as g
        """,
    )
)
# dbDataFrame = dbDataFrame.select(col("firstname"), col("lastname")).show()
print(2)
dbDataFrame = dbDataFrame.load()
print(3)
dbDataFrame = (
    dbDataFrame.withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .withColumn("id", F.expr("uuid()"))
)
print(4)
dbDataFrame.show(5)
