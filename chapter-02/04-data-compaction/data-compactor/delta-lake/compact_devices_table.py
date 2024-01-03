from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

from config import DemoConfiguration

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    devices_table = DeltaTable.forPath(spark_session, DemoConfiguration.DEVICES_TABLE)
    devices_table.optimize().executeCompaction()
