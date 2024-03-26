import os

from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == '__main__':
    spark = (SparkSession.builder.master('local[*]')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
             .getOrCreate())

    input_data = (spark.readStream.option('kafka.bootstrap.servers', 'localhost:9094')
                  .option('subscribe', 'visits')
                  .option('startingOffsets', 'EARLIEST')
                  .format('kafka')
                  .load())

    visits_events = (input_data.selectExpr('CAST(value AS STRING)')
                     .select(F.from_json('value', 'visit_id INT, event_time TIMESTAMP, page STRING').alias('visit'))
                     .selectExpr('visit.*'))

    session_window: DataFrame = (visits_events
                                 .withWatermark('event_time', '1 hour')
                                 .groupBy(F.window(F.col('event_time'), '10 minutes')).count())

    write_query = (session_window.withColumn('current_time', F.current_timestamp())
                   .writeStream
                   .format('console').option('truncate', False))

    write_query_started = write_query.start()
    write_query_started.processAllAvailable()


    def send_record(record_json_payload: str):
        (spark.createDataFrame([(record_json_payload,)], StructType([StructField('value', StringType())]))
         .write.format('kafka').option('kafka.bootstrap.servers', 'localhost:9094')
         .option('topic', 'visits').save())


    print(write_query_started.lastProgress['eventTime'])

    records_to_produce = [
        '{"visit_id": 1, "event_time": "2023-06-30T03:15:00Z", "page": "page1"}',
        '{"visit_id": 1, "event_time": "2023-06-30T03:00:00Z", "page": "page2"}',
        '{"visit_id": 1, "event_time": "2023-06-30T01:50:00Z", "page": "page3"}',
        '{"visit_id": 1, "event_time": "2023-06-30T03:11:00Z", "page": "page3"}',
        '{"visit_id": 1, "event_time": "2023-06-30T04:31:00Z", "page": "page3"}',
    ]

    for record in records_to_produce:
        print(f'Producing {record}')
        send_record(record)
        write_query_started.processAllAvailable()
        print(write_query_started.lastProgress['eventTime'])
    write_query_started.processAllAvailable()
