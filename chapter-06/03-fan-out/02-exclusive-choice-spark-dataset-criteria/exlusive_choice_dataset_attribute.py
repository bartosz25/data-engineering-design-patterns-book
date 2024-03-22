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
    # It's not a good choice to not specify the schema as it reads the schema twice!
    # But we're doing that just for the sake of this demo
    input_dataset = (spark_session.read.format('json').load(DemoConfiguration.INPUT_PATH))

    input_schema = input_dataset.schema
    output_location = DemoConfiguration.DEVICES_TABLE_LEGACY
    if len(input_schema.fields) >= 3:
        output_location = DemoConfiguration.DEVICES_TABLE_SCHEMA_CHANGED

    input_dataset.write.mode('append').format('delta').save(output_location)
