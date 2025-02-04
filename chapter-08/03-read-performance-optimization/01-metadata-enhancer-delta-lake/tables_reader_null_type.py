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

    count_delta = (spark_session.read.format('delta').load(DemoConfiguration.DEVICES_TABLE_DELTA_LAKE)
                   .filter('type IS NULL').count())
    count_json = (spark_session.read.schema('type STRING, full_name STRING, version STRING')
                  .json(DemoConfiguration.DEVICES_TABLE_JSON)
                  .filter('type IS NULL').count())

    print(f'Delta vs. JSON: {count_delta} vs. {count_json}')

    while True:
        pass