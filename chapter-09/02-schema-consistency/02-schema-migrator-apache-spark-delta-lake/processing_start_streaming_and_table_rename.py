import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.catalogImplementation", "hive")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    .config(
        "spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop",
        "always")
                                                    ).getOrCreate())

    checkpoint_dir = '/tmp/dedp/ch09/02-schema-consistency/02-schema-versioner-apache-spark-delta-lake/checkpoint-processing'

    visits_stream = (spark_session.readStream
                     .option('schemaTrackingLocation', f'{checkpoint_dir}/schema_tracking')
                     .table('default.visits'))

    stream_writer = (visits_stream
                     .withColumn('user_id_exists', functions.isnotnull('user_id'))
                     .writeStream
                     .option('checkpointLocation', checkpoint_dir)
                     .option('truncate', False).format('console').start())

    stream_writer.processAllAvailable()

    spark_session.sql('''
    ALTER TABLE default.visits RENAME COLUMN user_id TO user
    ''')

    spark_session.sql('DESCRIBE DETAIL default.visits').select('name', 'tableFeatures').show(truncate=False)
    spark_session.sql('SHOW TBLPROPERTIES default.visits').show(truncate=False)

    spark_session.sql('''
    INSERT INTO default.visits (visit_id, event_time, user, page) VALUES 
    ("visit 100", TIMESTAMP "2024-07-02T10:00:00.840Z", "user---10", "contact.html")
    ''')

    stream_writer.processAllAvailable()
