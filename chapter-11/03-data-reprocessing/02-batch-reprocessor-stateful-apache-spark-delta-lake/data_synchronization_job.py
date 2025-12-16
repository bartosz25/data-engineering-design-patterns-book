from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from config import CHECKPOINT_DIR_SYNCHRONIZER, DATA_DIR, INPUT_TOPIC_NAME
from schema import EVENT_SCHEMA

if __name__ == "__main__":
    spark_session =(configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())

    input_data = (spark_session.readStream.format('kafka') 
        .option('kafka.bootstrap.servers', 'localhost:9094') 
        .option('subscribe', INPUT_TOPIC_NAME).option('startingOffsets', 'EARLIEST') 
        .load())

    input_data_with_partition_columns = (input_data.selectExpr('CAST(value AS STRING)', 'partition', 'offset')
        .select(F.col('value'),
                F.from_json(F.col('value'), EVENT_SCHEMA).alias('data'),
                F.col('partition'), F.col('offset'))
        .withColumn('year', F.year(F.col('data.event_time')))
        .withColumn('month', F.month(F.col('data.event_time')))
        .withColumn('day', F.dayofmonth(F.col('data.event_time')))
        .withColumn('hour', F.hour(F.col('data.event_time')))
        .select('value', 'partition', 'offset', 'year', 'month', 'day', 'hour'))


    write_query = (input_data_with_partition_columns.writeStream
        .format("delta").partitionBy('year', 'month', 'day', 'hour')
        .option("checkpointLocation", CHECKPOINT_DIR_SYNCHRONIZER)
        .start(DATA_DIR))

    write_query.awaitTermination()
