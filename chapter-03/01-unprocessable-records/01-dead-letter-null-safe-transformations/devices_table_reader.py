from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

from config import get_base_dir


if __name__ == "__main__":
    base_dir = get_base_dir()

    spark_session = configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension") \
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                                                   ).getOrCreate()

    devices_table = DeltaTable.forPath(spark_session, f'{base_dir}/output/devices-table')

    devices_table.toDF().filter('name_with_version IS NOT NULL').show(truncate=False)
