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

    input_dataset.createTempView('devices_to_load')

    # for the record: https://spark.apache.org/docs/3.5.0/sql-ref-null-semantics.html#expressions-
    devices_to_load_with_validity_flag = spark_session.sql('''
    SELECT type, full_name, version, name_with_version, 
        CASE 
            WHEN (full_name IS NOT NULL OR version IS NOT NULL) AND name_with_version IS NULL THEN false
            ELSE true
        END AS is_valid
     FROM (SELECT type, full_name, version, CONCAT(full_name, version) AS name_with_version FROM devices_to_load)
    ''')

    devices_to_load_with_validity_flag.cache()

    (devices_to_load_with_validity_flag.filter('is_valid IS TRUE').drop('is_valid')
     .write.mode('overwrite').format('delta').save(f'{base_dir}/output/devices-table'))

    (devices_to_load_with_validity_flag.filter('is_valid IS FALSE').drop('is_valid')
     .write.mode('overwrite').format('delta').save(f'{base_dir}/output/devices-dead-letter-table'))
