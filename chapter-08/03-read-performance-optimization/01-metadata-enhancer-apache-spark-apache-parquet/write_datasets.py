from pyspark.sql import SparkSession, functions

from config import get_base_dir, get_parquet_dir, get_json_dir

if __name__ == "__main__":
    base_dir = get_base_dir()

    spark_session = SparkSession.builder.master("local[*]").getOrCreate()

    user_schema = '''
    id STRING, login STRING, email STRING, registered_datetime TIMESTAMP,
    first_connection_datetime TIMESTAMP, last_connection_datetime TIMESTAMP
    '''
    input_dataset = (spark_session.read.schema(user_schema).json(f'{base_dir}/input')
                     .sort(functions.asc_nulls_first('first_connection_datetime')))
    input_dataset.persist()
    input_dataset.repartition(10).write.mode('overwrite').parquet(path=get_parquet_dir())
    input_dataset.repartition(10).write.mode('overwrite').json(path=get_json_dir())
