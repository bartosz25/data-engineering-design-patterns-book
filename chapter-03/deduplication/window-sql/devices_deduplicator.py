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
    input_dataset.createTempView('duplicated_devices')

    deduplicated_devices = spark_session.sql('''
    SELECT type, full_name, version FROM (
     SELECT type, full_name, version, ROW_NUMBER() OVER (PARTITION BY type, full_name, version ORDER BY 1) AS position
     FROM duplicated_devices
    ) 
    WHERE position = 1
    ''')

    deduplicated_devices.write.mode('overwrite').format('delta').save(f'{base_dir}/devices-table-from-window')
