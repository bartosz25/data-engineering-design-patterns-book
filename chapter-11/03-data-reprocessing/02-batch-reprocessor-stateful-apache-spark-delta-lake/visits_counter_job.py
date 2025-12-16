from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from config import CHECKPOINT_DIR_STREAMING_JOB, INPUT_TOPIC_NAME, OUTPUT_TOPIC_NAME
from schema import EVENT_SCHEMA
from shared_job_logic import generate_windowed_stats

if __name__ == "__main__":
    spark_session =(configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())

    input_stream_data = (spark_session.readStream.format('kafka')
                         .option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('subscribe', INPUT_TOPIC_NAME).option('startingOffsets', 'EARLIEST')
                         .load())

    input_stream_with_watermark = (input_stream_data
                                   .selectExpr('CAST(value AS STRING)')
                                   .select(F.from_json("value", 'event_time TIMESTAMP').alias("value"))
                                   .selectExpr('value.*')
                                   .withWatermark('event_time', '10 minutes'))
    windowed_stats = generate_windowed_stats(input_stream_with_watermark)

    write_query = (windowed_stats.writeStream.format('kafka')
                   .option('topic', OUTPUT_TOPIC_NAME)
                   .option('kafka.bootstrap.servers', 'localhost:9094')
                   .option('checkpointLocation', CHECKPOINT_DIR_STREAMING_JOB)
                   .start())

    write_query.awaitTermination()
