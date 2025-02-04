from delta import configure_spark_with_delta_pip
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import get_visit_event_schema, get_staging_visits_table

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


    def write_dataset_to_staging_table(visits_to_audit: DataFrame, batch_version: int):
        (visits_to_audit
         .withColumn('batch_version', F.lit(batch_version))
         .write.format('delta').insertInto(get_staging_visits_table()))


    write_query = (visits.writeStream
                   .trigger(processingTime='15 seconds')
                   .option('checkpointLocation',
                           '/tmp/dedp/ch09/01-quality-enforcement/01-audit-write-audit-publish-apache-kafka-apache-spark-delta-lake-staging/writer/checkpoint')
                   .outputMode('append').foreachBatch(write_dataset_to_staging_table).start())

    write_query.awaitTermination()
