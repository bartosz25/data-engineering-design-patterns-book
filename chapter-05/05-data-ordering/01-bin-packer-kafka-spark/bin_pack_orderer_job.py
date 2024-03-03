from pyspark.sql import SparkSession, functions as F, DataFrame

from bin_pack_orderer_writer_to_kafka import write_records_to_kafka

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = spark.readStream \
        .option('kafka.bootstrap.servers', 'localhost:9094') \
        .option('subscribe', 'visits') \
        .option('startingOffsets', 'LATEST') \
        .format('kafka').load()

    visit_schema = 'visit_id STRING, event_time TIMESTAMP'

    visits_from_kafka: DataFrame = (input_data_stream
                                    .select(F.from_json(F.col('value').cast('string'), visit_schema).alias('visit'),
                                            'value')
                                    .selectExpr('visit.*', 'CAST(value AS STRING)'))

    def write_sorted_events(events: DataFrame, batch_number: int):
        # No need for grouping; visits are already correctly partitioned
        (events.sortWithinPartitions([F.col('visit_id'), F.col('event_time')])
         .foreachPartition(lambda rows: write_records_to_kafka(bootstrap_server='localhost:9094',
                                                                   output_topic='visits-ordered', visits_rows=rows)))

    write_data_stream = (visits_from_kafka.writeStream
                         .trigger(processingTime='20 seconds') # 20 seconds to get as many events for a visit at once
                         .option('checkpointLocation',
                                 '/tmp/dedp/ch05/05-data-ordering/01-bin-packer-kafka-spark/checkpoint-bin-pack-orderer')
                         .foreachBatch(write_sorted_events))

    write_data_stream.start().awaitTermination()
