from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = spark.readStream \
        .option('kafka.bootstrap.servers', 'localhost:9094') \
        .option('subscribe', 'events') \
        .option("includeHeaders", 'true') \
        .option("startingOffsets", 'EARLIEST') \
        .format('kafka').load()

    events_to_replicate = (input_data_stream
                           .selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)', 'partition', 'headers',
                                       'offset'))

    write_data_stream = events_to_replicate.writeStream.format('console').option('truncate', 'false')

    write_data_stream.start().awaitTermination()
