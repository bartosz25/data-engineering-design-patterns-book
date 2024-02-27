from datetime import datetime

from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructField, IntegerType, LongType, TimestampType, StringType, ArrayType, \
    StructType

from sessions_mapper import map_visits_to_session

if __name__ == '__main__':
    spark_session = (SparkSession.builder.master("local[*]")
                     .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
                     .config("spark.sql.shuffle.partitions", 2).getOrCreate())

    input_data_stream = (spark_session.readStream
                         .option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('subscribe', 'visits')
                         .option('startingOffsets', 'LATEST')
                         .format('kafka').load())

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

    grouped_visits = (visits_from_kafka
                      .withWatermark('event_time', '1 minute')
                      .groupBy(F.col('visit_id')))

    visited_pages_type = ArrayType(StructType([
        StructField("page", StringType()),
        StructField("event_time_as_milliseconds", LongType())
    ]))

    sessions = grouped_visits.applyInPandasWithState(
        func=map_visits_to_session,
        outputStructType=StructType([
            StructField("visit_id", StringType()),
            StructField("user_id", StringType()),
            StructField("start_time", TimestampType()),
            StructField("end_time", TimestampType()),
            StructField("visited_pages", visited_pages_type),
            StructField("duration_in_milliseconds", LongType())
        ]),
        stateStructType=StructType([
            StructField("visits", visited_pages_type),
            StructField("user_id", StringType())
        ]),
        outputMode="update",
        timeoutConf="EventTimeTimeout"
    )

    visits_to_write = sessions.withColumn('value', F.to_json(F.struct('*'))).select('value')

    write_query = (visits_to_write.writeStream.outputMode("update")
                   .option("checkpointLocation", '/tmp/dedp/chapter-05/04-sessionization/02-stateful-kafka-spark')
                   .format("kafka")
                   .option('kafka.bootstrap.servers', 'localhost:9094')
                   .option('topic', 'sessions')
                   .start())

    write_query.awaitTermination()
