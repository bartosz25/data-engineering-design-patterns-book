from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame, functions as F

from config import OUTPUT_TOPIC_NAME

if __name__ == "__main__":
    spark_session =(configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())

    input_data = (spark_session.read.format('kafka')
        .option('kafka.bootstrap.servers', 'localhost:9094')
        .option('subscribe', OUTPUT_TOPIC_NAME).option('startingOffsets', 'EARLIEST')
        .load())

    data_to_display = input_data.selectExpr('CAST(value AS STRING)', 'partition', 'offset')

    data_to_display.orderBy(F.asc('partition'), F.asc('offset')).show(n=2000, truncate=False)
