from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import DemoConfiguration

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    input_delta_dataset = spark_session.read.format('delta').load(DemoConfiguration.DEVICES_TABLE_PATH)

    devices_without_full_name = input_delta_dataset.drop('full_name')

    devices_without_full_name.write.format('delta').save(DemoConfiguration.DEVICES_TABLE_NO_NAME_PATH)
