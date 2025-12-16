from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_base_dir, get_devices_table_dir, get_visits_enriched_raw_table_dir, get_devices_stats_table_dir

if __name__ == "__main__":
    spark_session =(configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                   ).getOrCreate())

    print('Visits enriched')
    spark_session.read.format('delta').load(get_visits_enriched_raw_table_dir()).show(truncate=False, n=5)

    print('Devices stats')
    spark_session.read.format('delta').load(get_devices_stats_table_dir()).show(truncate=False, n=5)
