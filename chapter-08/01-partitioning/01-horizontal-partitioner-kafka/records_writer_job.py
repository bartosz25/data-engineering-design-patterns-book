from pyspark import Row
from pyspark.sql import SparkSession, functions

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_users = spark.createDataFrame(data=[
        Row(user_id=1, country='Poland'),
        Row(user_id=2, country='France'),
        Row(user_id=3, country='the USA'),
        Row(user_id=4, country='Spain'),
        Row(user_id=1, country='Spain'),
        Row(user_id=2, country='Poland'),
        Row(user_id=4, country='France')
    ])

    input_users_for_kafka = (input_users
                             .withColumn('value', functions.to_json(functions.struct('*')))
                             .withColumn('key', functions.col('user_id').cast('string'))
                             .select('key', 'value'))

    (input_users_for_kafka.write.format('kafka')
     .option('kafka.bootstrap.servers', 'localhost:9094').option('topic', 'users')
     .save())

