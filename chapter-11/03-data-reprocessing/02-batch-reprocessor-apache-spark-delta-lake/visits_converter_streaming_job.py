from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import CHECKPOINT_DIR_STREAMING_JOB, INPUT_TOPIC_NAME, OUTPUT_TOPIC_NAME
from shared_job_logic import rename_columns

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

    input_with_extracted_columns = rename_columns(input_stream_data)

    write_query = (input_with_extracted_columns.writeStream.format('kafka')
                   .option('topic', OUTPUT_TOPIC_NAME)
                   .option('kafka.bootstrap.servers', 'localhost:9094')
                   .option('checkpointLocation', CHECKPOINT_DIR_STREAMING_JOB)
                   .start())

    write_query.awaitTermination()
