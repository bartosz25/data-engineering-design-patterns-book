from pyspark.sql import SparkSession, functions as F

def load_real_time_data(spark_session: SparkSession):
    visit_schema = 'visit_id STRING, user_id STRING, page STRING'
    input_data_stream = (spark_session.readStream
        .option('kafka.bootstrap.servers', 'localhost:9094')
        .option('subscribe', 'visits')
        .option('maxOffsetsPerTrigger', 10)
        .option('startingOffsets', 'EARLIEST')
        .format('kafka').load())

    visits_from_kafka = (input_data_stream
                                    .select(F.from_json(F.col('value').cast('string'), visit_schema).alias('visit'), 'value')
                                    .selectExpr('visit.*'))

    return visits_from_kafka

