import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                   ).getOrCreate())
    partition = sys.argv[1]
    base_dir = '/tmp/dedp/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-sequential/dataset'
    input_path = f'{base_dir}/input'
    devices_table_path = f'{base_dir}/devices'
    input_dataset = (spark_session.read.schema('type STRING, full_name STRING, version STRING').format('json')
                     .load(input_path)
                     .withColumn('event_time', functions.lit(partition)))

    input_dataset.write.partitionBy('event_time').mode('append').format('delta').save(devices_table_path)
