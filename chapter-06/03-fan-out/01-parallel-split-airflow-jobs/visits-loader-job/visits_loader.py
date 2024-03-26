from enum import Enum

from pyspark.sql import SparkSession, functions
import argparse

from pyspark.sql.functions import to_date, col

if __name__ == "__main__":
    class OutputFormat(str, Enum):
        delta_lake = 'delta'
        csv = 'csv'


    parser = argparse.ArgumentParser(prog='Visits loader')
    parser.add_argument('--input_dir', required=True)
    parser.add_argument('--format', required=True, type=OutputFormat)
    parser.add_argument('--output_dir', required=True)
    job_arguments = parser.parse_args()

    if job_arguments.format == OutputFormat.delta_lake:
        spark_session = (SparkSession.builder
                         .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                         .config('spark.sql.catalog.spark_catalog',
                                 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                         .getOrCreate())
    else:
        spark_session = SparkSession.builder.getOrCreate()

    input_data_schema = '''
    visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
        context STRUCT<
            referral STRING, ad_id STRING, 
            user STRUCT<
                ip STRING, login STRING, connected_since TIMESTAMP
            >,
            technical STRUCT<
                browser STRING, browser_version STRING, network_type STRING, device_type STRING, device_version STRING
            >
        >
    '''
    input_data = (spark_session.read.schema(input_data_schema)
                  .json(job_arguments.input_dir).withColumn('event_date', to_date(col('event_time'))))

    input_data_to_write = input_data
    if job_arguments.format == OutputFormat.csv:
        input_data_to_write = input_data.withColumn('context', functions.to_json(col('context')))

    input_data_to_write.write.partitionBy('event_date').mode('append').format(job_arguments.format).save(job_arguments.output_dir)
