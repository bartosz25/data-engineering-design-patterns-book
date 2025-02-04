from pyspark.sql import SparkSession

from config import get_json_table_dir

if __name__ == '__main__':
    spark_session = SparkSession.builder.master("local[*]").getOrCreate()

    users_from_delta = spark_session.read.format('json').load(get_json_table_dir())
    users_from_delta.show(truncate=False)
