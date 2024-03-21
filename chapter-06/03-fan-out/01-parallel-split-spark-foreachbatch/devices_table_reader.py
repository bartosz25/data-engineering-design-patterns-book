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

    print('------------- Raw table --------------')
    devices_table = DeltaTable.forPath(spark_session, DemoConfiguration.DEVICES_TABLE)
    device_table_df = devices_table.toDF()
    device_table_df.show(truncate=False)
    print(f'All rows={device_table_df.count()}')

    print('------------- Enriched table --------------')
    devices_table_enriched = DeltaTable.forPath(spark_session, DemoConfiguration.DEVICES_TABLE_ENRICHED)
    devices_table_enriched_df = devices_table_enriched.toDF()
    devices_table_enriched_df.show(truncate=False)
    print(f'All rows={devices_table_enriched_df.count()}')
