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

    row_validation_expression = F.expr('''
    CASE 
        WHEN event_time IS NULL OR user_id IS NULL OR page IS NULL or context IS NULL THEN
            false
        ELSE
            true
    END
    ''')

    visits = (spark_session.readStream
              .option('kafka.bootstrap.servers', 'localhost:9094')
              .option('subscribe', 'visits')
              .option('startingOffsets', 'EARLIEST')
              .option('maxOffsetsPerTrigger', '50')
              .format('kafka').load()
              .selectExpr('CAST(value AS STRING)')
              .select(F.from_json("value", get_visit_event_schema()).alias("visit"), "value")
              .selectExpr('visit.*')
              .withColumn('is_valid', row_validation_expression)
              )


    def publish_dataset_if_valid(visits_to_audit: DataFrame, batch_version: int):
        visits_to_audit.persist()

        should_write_to_valid_table = True
        all_visits = visits_to_audit.count()
        # The threshold here is static to keep the example simple but IRL it might use statistics from the past runs
        if 0 < all_visits < 10000:
            error_rows = visits_to_audit.filter('is_valid = FALSE').count()
            accepted_errors_percentage = 25
            # We promote valid rows only if they are overall 75% part of the whole dataset
            errors_percentage = error_rows * 100 / all_visits
            if errors_percentage > accepted_errors_percentage:
                should_write_to_valid_table = False
        else:
            should_write_to_valid_table = False

        def decorate_visits_with_batch_version(visits_to_decorate: DataFrame):
            return visits_to_decorate.withColumn('batch_version', F.lit(batch_version))

        if should_write_to_valid_table:
            (visits_to_audit.filter('is_valid = TRUE').drop('is_valid')
             .write.format('delta').insertInto(get_valid_visits_table()))
            (decorate_visits_with_batch_version(visits_to_audit.filter('is_valid = FALSE')).drop('is_valid')
             .write.format('delta').insertInto(get_error_table()))
        else:
            (decorate_visits_with_batch_version(visits_to_audit).drop('is_valid')
             .write.format('delta').insertInto(get_error_table()))

        visits_to_audit.unpersist()


    write_query = (visits.writeStream
                   .trigger(processingTime='15 seconds')
                   .option('checkpointLocation',
                           '/tmp/dedp/ch09/01-quality-enforcement/01-audit-write-audit-publish-apache-kafka-apache-spark-delta-lake/checkpoint')
                   .outputMode('append').foreachBatch(publish_dataset_if_valid).start())

    write_query.awaitTermination()
