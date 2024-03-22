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

    input_schema = 'type STRING, full_name STRING, version STRING'

    input_dataset_1 = (spark_session.read.schema(input_schema).format('json').load(DemoConfiguration.INPUT_PATH_1)
                       .withColumn('dataset', functions.lit('dataset_1')))
    input_dataset_2 = (spark_session.read.schema(input_schema).format('json').load(DemoConfiguration.INPUT_PATH_2)
                       .withColumn('dataset', functions.lit('dataset_2')))

    output_dataset = input_dataset_1.unionByName(input_dataset_2)
    output_dataset.write.mode('overwrite').format('delta').save(DemoConfiguration.DEVICES_TABLE)
