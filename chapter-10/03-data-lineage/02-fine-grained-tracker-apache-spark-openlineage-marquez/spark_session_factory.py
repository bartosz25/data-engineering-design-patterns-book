from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session_with_delta_lake_and_open_lineage(app_name: str) -> SparkSession:
    return configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                          .appName(app_name)
                                          .config("spark.sql.catalogImplementation", "hive")
                                          .config("spark.sql.extensions",
                                                  "io.delta.sql.DeltaSparkSessionExtension")
                                          .config("spark.sql.catalog.spark_catalog",
                                                  "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                          .config("spark.extraListeners",
                                                  "io.openlineage.spark.agent.OpenLineageSparkListener")
                                          .config("spark.openlineage.transport.type", "http")
                                          .config("spark.openlineage.transport.url", "http://localhost:5000")
                                          .config("spark.openlineage.namespace", "visits"),
                                          extra_packages=['io.openlineage:openlineage-spark_2.12:1.21.1']
                                          ).getOrCreate()
