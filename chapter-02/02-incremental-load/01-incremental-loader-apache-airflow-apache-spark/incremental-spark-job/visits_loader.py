from pyspark.sql import SparkSession
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='Incremental loader')
    parser.add_argument('--input_dir', required=True)
    parser.add_argument('--output_dir', required=True)
    job_arguments = parser.parse_args()

    spark_session = SparkSession.builder.getOrCreate()

    input_data = spark_session.read.text(job_arguments.input_dir)

    input_data.write.mode('overwrite').text(job_arguments.output_dir)
