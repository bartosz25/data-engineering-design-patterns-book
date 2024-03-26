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

    devices_table = DeltaTable.forPath(spark_session, f'{base_dir}/devices-valid-table')

    # should be empty, as the null rows should be filtered out before
    print('Empty rows')
    devices_table.toDF().filter('type IS NULL OR full_name IS NULL OR version IS NULL').show(truncate=False)

    print('Not empty rows')
    devices_table.toDF().filter('type IS NOT NULL AND full_name IS NOT NULL AND version IS NOT NULL').show(truncate=False)
    print(f"Rows={devices_table.toDF().filter('type IS NOT NULL AND full_name IS NOT NULL AND version IS NOT NULL').count()}")
    print(f"Rows={devices_table.toDF().count()}")
