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

    input_dataset = (spark_session.read.schema('type STRING, full_name STRING, version STRING').format('json')
                     .load(DemoConfiguration.INPUT_PATH))
    input_dataset.cache()

    input_dataset.write.mode('overwrite').format('delta').save(DemoConfiguration.DEVICES_TABLE)
    input_dataset.write.mode('append').format('delta').save(DemoConfiguration.DEVICES_TABLE)
    input_dataset.write.mode('append').format('delta').save(DemoConfiguration.DEVICES_TABLE)
