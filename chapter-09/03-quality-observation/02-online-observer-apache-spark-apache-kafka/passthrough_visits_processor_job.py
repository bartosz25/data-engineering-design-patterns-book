from pyspark.sql import SparkSession

from kafka_sink_with_observer import write_to_kafka_with_observer

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[2]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = (spark.readStream
                         .option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('subscribe', 'visits')
                         .option('startingOffsets', 'EARLIEST')
                         .format('kafka').load())

    visits = (input_data_stream
              .selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)', 'offset', 'partition'))

    query = (visits.writeStream
             .foreachBatch(write_to_kafka_with_observer)
             .option('kafka.bootstrap.servers', 'localhost:9094')
             .option('checkpointLocation',
                     '/tmp/dedp/chapter-09/03-quality-observation/02-online-observer-apache-spark-apache-kafka/checkpoint')
             .start())

    query.awaitTermination()
