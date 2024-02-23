from typing import Iterator

from pyspark import Row
from pyspark.sql import SparkSession, functions as F, DataFrame

from kafka_writer import KafkaWriter

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = spark.readStream \
        .option('kafka.bootstrap.servers', 'localhost:9094') \
        .option('subscribe', 'visits') \
        .option('startingOffsets', 'LATEST') \
        .format('kafka').load()

    visit_schema = '''
        visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
        context STRUCT<
            referral STRING, ad_id STRING, 
            user STRUCT<
                ip STRING, login STRING, connected_since TIMESTAMP
            >,
            technical STRUCT<
                browser STRING, browser_version STRING, network_type STRING, device_type STRING, device_version STRING
            >
        >
    '''
    visits_from_kafka: DataFrame = (input_data_stream
                                    .select(F.from_json(F.col('value').cast('string'), visit_schema).alias('value'))
                                    .selectExpr('value.*'))


    def write_aggregated_visits_to_kafka(visits_to_save: DataFrame, batch_number: int):
        sorted_visits: DataFrame = visits_to_save.sortWithinPartitions(['visit_id', 'event_time'])

        def write_records_from_spark_partition_to_kafka_topic(visits: Iterator[Row]):
            kafka_writer = KafkaWriter(bootstrap_server='localhost:9094', output_topic='visits-aggregated')
            for visit in visits:
                kafka_writer.process(visit)
            kafka_writer.close()

        sorted_visits.foreachPartition(write_records_from_spark_partition_to_kafka_topic)


    write_data_stream = (visits_from_kafka.writeStream
                         .trigger(processingTime='10 seconds')
                         .option('checkpointLocation',
                                 '/tmp/dedp/ch05/03-data-combination/02-local-kafka-spark/checkpoint')
                         .foreachBatch(write_aggregated_visits_to_kafka))

    write_data_stream.start().awaitTermination()
