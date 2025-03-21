import datetime

from delta import configure_spark_with_delta_pip
from pyspark import Row
from pyspark.sql import SparkSession, functions

from config import get_delta_table_dir

if __name__ == '__main__':
    spark_session = configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension") \
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                                                   ).getOrCreate()

    input_users = spark_session.createDataFrame(data=[
        Row(user_id=1, country='Poland', change_date=datetime.datetime(2024, 5, 25, 10)),
        Row(user_id=2, country='France', change_date=datetime.datetime(2024, 5, 25, 10)),
        Row(user_id=3, country='the USA', change_date=datetime.datetime(2024, 5, 25, 10)),
        Row(user_id=4, country='Spain', change_date=datetime.datetime(2024, 5, 25, 5)),
        Row(user_id=1, country='Spain', change_date=datetime.datetime(2024, 5, 26, 3)),
        Row(user_id=2, country='Poland', change_date=datetime.datetime(2024, 5, 27, 10)),
        Row(user_id=4, country='France', change_date=datetime.datetime(2024, 5, 27, 11))
    ])

    users_with_partition_values = (input_users
                                   .withColumn('year', functions.year('change_date'))
                                   .withColumn('month', functions.month('change_date'))
                                   .withColumn('day', functions.day('change_date'))
                                   .withColumn('hour', functions.hour('change_date')))

    (users_with_partition_values.write.mode('overwrite').format('delta').partitionBy('year', 'month', 'day', 'hour')
     .save(get_delta_table_dir()))

