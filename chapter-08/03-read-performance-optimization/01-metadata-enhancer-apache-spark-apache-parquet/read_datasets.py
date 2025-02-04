from pyspark.sql import SparkSession

from config import get_base_dir, get_parquet_dir, get_json_dir

if __name__ == "__main__":
    base_dir = get_base_dir()

    spark_session = (SparkSession.builder.master('local[*]').getOrCreate())

    #spark_session.read.parquet(get_parquet_dir()).filter('first_connection_datetime IS NOT NULL').count()
    spark_session.read.json(get_json_dir()).filter('first_connection_datetime IS NOT NULL').count()

    while True:
        pass