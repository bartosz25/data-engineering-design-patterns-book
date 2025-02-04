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
    for run in range(0, 3):
        spark_session.sql(f'''
        INSERT INTO default.visits (visit_id, event_time, user_id, page) VALUES 
        ("visit 1-{run}", TIMESTAMP "2024-07-01T10:00:00.840Z", "user 1", "index.html"),
        ("visit 2-{run}", TIMESTAMP "2024-07-01T12:00:00.840Z", "user 2", "contact.html"),
        ("visit 3-{run}", TIMESTAMP "2024-07-01T13:00:00.840Z", "user 3", "about.html")
        ''')

    last_written_version = (spark_session.sql('DESCRIBE HISTORY default.visits')
                            .selectExpr('MAX(version) AS last_version').collect()[0].last_version)
    registry = CollectorRegistry()
    metrics_gauge = Gauge('visits_writer_version',
                          'Last version of the visits table', registry=registry)
    metrics_gauge.set(last_written_version)
    push_to_gateway('localhost:9091', job='visits_writer_version', registry=registry)