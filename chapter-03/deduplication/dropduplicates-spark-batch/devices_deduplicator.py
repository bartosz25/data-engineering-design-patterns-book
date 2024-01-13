from delta import configure_spark_with_delta_pip
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

    input_dataset = (spark_session.read.schema('type STRING, full_name STRING, version STRING').format('json')
                     .load(f'{base_dir}/input'))

    input_dataset_deduplicated = input_dataset.dropDuplicates(['type', 'full_name', 'version'])

    input_dataset_deduplicated.write.mode('overwrite').format('delta').save(f'{base_dir}/devices-table')
