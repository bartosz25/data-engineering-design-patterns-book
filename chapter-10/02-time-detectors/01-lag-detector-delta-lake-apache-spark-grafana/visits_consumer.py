import json
import os

from delta import configure_spark_with_delta_pip
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.catalogImplementation", "hive")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    checkpoint_dir = '/tmp/dedp/ch10/02-time-detectors/01-lag-detector-delta-lake-apache-spark-grafana/checkpoint'

    visits_stream = spark_session.readStream.table('default.visits')

    console_printer = (visits_stream.writeStream
                       .trigger(availableNow=True)
                       .option('checkpointLocation', checkpoint_dir)
                       .option('truncate', False).format('console'))

    query = console_printer.start()
    query.awaitTermination()

    last_processed_version = query.lastProgress["sources"][0]["endOffset"]["reservoirVersion"]

    registry = CollectorRegistry()
    metrics_gauge = Gauge('visits_reader_version',
                          'Last read version of the visits table', registry=registry)
    metrics_gauge.set(last_processed_version)
    push_to_gateway('localhost:9091', job='visits_reader_version', registry=registry)
