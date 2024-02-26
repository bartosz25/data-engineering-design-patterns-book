from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_base_dir, get_devices_table_dir

if __name__ == "__main__":
    spark_session = configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension") \
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                                                   ).getOrCreate()

    input_dataset = (spark_session.read.schema('type STRING, full_name STRING, version STRING').format('json')
                     .load(f'{get_base_dir()}/input'))

    input_dataset.write.mode('overwrite').format('delta').save(get_devices_table_dir())
