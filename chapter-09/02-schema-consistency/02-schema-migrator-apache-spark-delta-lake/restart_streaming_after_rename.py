from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

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

    checkpoint_dir = '/tmp/dedp/ch09/02-schema-consistency/02-schema-versioner-apache-spark-delta-lake/checkpoint'

    visits_stream = (spark_session.readStream
                     .option('schemaTrackingLocation', f'{checkpoint_dir}/schema_tracking')
                     .table('default.visits'))

    stream_writer = (visits_stream.writeStream
                     .option('checkpointLocation', checkpoint_dir)
                     .option('truncate', False).format('console').start())

    stream_writer.processAllAvailable()
