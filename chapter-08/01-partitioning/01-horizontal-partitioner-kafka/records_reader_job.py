from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = (spark.read
        .option('kafka.bootstrap.servers', 'localhost:9094')
        .option('subscribe', 'users')
        .option('startingOffsets', 'EARLIEST')
        .format('kafka').load())

    users_from_kafka = (input_data_stream
                        .selectExpr('partition', 'CAST(key AS STRING)', 'CAST(value AS STRING)'))

    users_from_kafka.show(truncate=False)

