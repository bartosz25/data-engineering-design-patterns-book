from pyspark.sql import SparkSession, DataFrame

from config import get_base_dir
from kafka_writer_bulk_send import KafkaWriterBulkSend

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = spark.readStream \
        .option('kafka.bootstrap.servers', 'localhost:9094') \
        .option('subscribe', 'visits') \
        .option('startingOffsets', 'LATEST') \
        .format('kafka').load()

    visits_from_kafka: DataFrame = input_data_stream.selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)')

    write_data_stream = (visits_from_kafka.writeStream
                         .trigger(processingTime='5 seconds')
                         .option('checkpointLocation',
                                 f'{get_base_dir()}/checkpoint-bulk-send')
                         .foreach(KafkaWriterBulkSend(bootstrap_server='localhost:9094',
                                                      output_topic='visits-batched')))

    write_data_stream.start().awaitTermination()
