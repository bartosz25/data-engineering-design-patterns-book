from pyspark.sql import SparkSession, DataFrame

from config import DemoConfiguration

if __name__ == "__main__":
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = spark.readStream \
        .option('kafka.bootstrap.servers', 'localhost:9094') \
        .option('subscribe', 'events') \
        .option("includeHeaders", 'true') \
        .format('kafka').load()

    events_to_replicate = (input_data_stream
                           .selectExpr('key', 'value', 'partition', 'headers', 'offset'))


    def write_sorted_events(events: DataFrame, batch_number: int):
        (events.sortWithinPartitions('offset', ascending=True).drop('offset').write.format('kafka')
         .option('kafka.bootstrap.servers', 'localhost:9094')
         .option('topic', 'events-replicated').option('includeHeaders', 'true').save())


    write_data_stream = (events_to_replicate.writeStream
                         .option('checkpointLocation', f'{DemoConfiguration.BASE_DIR}/checkpoint-kafka-replicator')
                         .foreachBatch(write_sorted_events))

    write_data_stream.start().awaitTermination()
