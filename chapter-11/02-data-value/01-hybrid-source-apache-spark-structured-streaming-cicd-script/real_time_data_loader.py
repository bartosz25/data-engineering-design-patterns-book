from pyspark.sql import SparkSession, functions as F

def load_real_time_data(spark_session: SparkSession):
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
    input_data_stream = (spark_session.readStream
        .option('kafka.bootstrap.servers', 'localhost:9094')
        .option('subscribe', 'visits')
        .option('startingOffsets', 'EARLIEST')
        .format('kafka').load())

    visits_from_kafka = (input_data_stream
                                    .select(F.from_json(F.col('value').cast('string'), visit_schema).alias('visit'), 'value')
                                    .selectExpr('visit.*'))

    streaming_query = (visits_from_kafka.writeStream
                       .option('checkpointLocation', '/tmp/dedp/ch11/11-streaming/02-hybrid-source-apache-spark-structured-streaming-cicd-script/checkpoint')
     .format('delta').start('/tmp/dedp/ch11/11-streaming/02-hybrid-source-apache-spark-structured-streaming-cicd-script/delta_table'))

    streaming_query.awaitTermination()
