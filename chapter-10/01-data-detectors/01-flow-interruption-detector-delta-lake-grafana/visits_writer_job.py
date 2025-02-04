import pandas
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import get_visit_event_schema, get_valid_visits_table, get_error_table

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


    def write_to_delta_lake_table_and_prometheus_pushgateway(visits_to_write: DataFrame, batch_version: int):
        visits_to_write.write.format('delta').insertInto(get_valid_visits_table())
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
        try:
            registry = CollectorRegistry()
            metrics_gauge = Gauge('visits_last_update_time',
                                  'Update time for the visits Delta Lake table', registry=registry)
            metrics_gauge.set_to_current_time()
            metrics_gauge.set(1)
            push_to_gateway('localhost:9091', job='visits_table_ingestor', registry=registry)
        except Exception as error:
            print(f'An error occurred for sending the gauge: {error}')


    write_query = (visits.writeStream
                   .trigger(processingTime='15 seconds')
                   .option('checkpointLocation',
                           '/tmp/dedp/ch10/01-data-detectors/01-flow-interruption-detector-delta-lake-grafana/checkpoint')
                   .outputMode('append').foreachBatch(write_to_delta_lake_table_and_prometheus_pushgateway).start())

    write_query.awaitTermination()
