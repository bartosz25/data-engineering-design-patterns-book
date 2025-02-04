import time

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import get_visit_event_schema, get_valid_visits_table
from stream_listeners import BatchCompletionSlaListener

if __name__ == "__main__":
    spark_session_builder = (SparkSession.builder.master('local[*]')
                             .config("spark.sql.shuffle.partitions", 2)
                             .config("spark.sql.session.timeZone", "UTC")
                             .config("spark.sql.catalogImplementation", "hive")
                             .config('spark.sql.extensions',
                                     'io.delta.sql.DeltaSparkSessionExtension')
                             .config('spark.sql.catalog.spark_catalog',
                                     'org.apache.spark.sql.delta.catalog.DeltaCatalog'))
    spark_session = (configure_spark_with_delta_pip(spark_session_builder=spark_session_builder,
                                                    extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0']
                                                    ).getOrCreate())

    visits = (spark_session.readStream
              .option('kafka.bootstrap.servers', 'localhost:9094')
              .option('subscribe', 'visits')
              .option('startingOffsets', 'EARLIEST')
              .option('maxOffsetsPerTrigger', '50')
              .format('kafka').load()
              .selectExpr('CAST(value AS STRING)')
              .select(F.from_json("value", get_visit_event_schema()).alias("visit"), "value")
              .selectExpr('visit.*')
              )


    def write_to_delta_table(visits_to_save: DataFrame, batch_number: int):
        time.sleep(15)
        visits_to_save.write.insertInto(get_valid_visits_table())

    write_query = (visits.writeStream
                   .trigger(processingTime='15 seconds')
                   .option('checkpointLocation',
                           '/tmp/dedp/ch10/02-time-detectors/02-sla-misses-apache-spark-structured-streaming/checkpoint')
                   .outputMode('append').foreachBatch(write_to_delta_table).start())


    spark_session.streams.addListener(BatchCompletionSlaListener())

    write_query.awaitTermination()
