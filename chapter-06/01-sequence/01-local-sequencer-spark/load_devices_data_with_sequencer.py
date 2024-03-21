from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions

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

    valid_and_enriched_dataset_to_write = (input_dataset.filter(functions.length(input_dataset.type) > 3)
                                           .filter('full_name IS NOT NULL')
                                           .withColumn('full_name', functions.concat_ws(' ',
                                                                                        input_dataset.full_name,
                                                                                        input_dataset.version)))

    valid_and_enriched_dataset_to_write.write.mode('overwrite').format('delta').save(DemoConfiguration.DEVICES_TABLE)
