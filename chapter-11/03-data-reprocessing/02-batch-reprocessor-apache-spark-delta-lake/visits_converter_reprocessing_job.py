import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import DATA_DIR, OUTPUT_TOPIC_NAME
from shared_job_logic import rename_columns

if __name__ == "__main__":
    start_offset = sys.argv[1]
    end_offset = sys.argv[2]

    spark_session =(configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())

    reprocessed_data = spark_session.read.format('delta').load(DATA_DIR).filter(f'''
        offset >= {start_offset} AND offset <= {end_offset}
    ''')

    input_with_extracted_columns = rename_columns(reprocessed_data)

    write_query = (input_with_extracted_columns.write.format('kafka')
                   .option('topic', OUTPUT_TOPIC_NAME)
                   .option('kafka.bootstrap.servers', 'localhost:9094')
                   .save())
