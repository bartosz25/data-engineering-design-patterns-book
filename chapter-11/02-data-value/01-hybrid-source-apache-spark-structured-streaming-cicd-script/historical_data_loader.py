from pyspark.sql import SparkSession

def load_historical_data(spark_session: SparkSession):
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

    historical_data_dir = '/tmp/dedp/ch11/11-streaming/02-hybrid-source-apache-spark-structured-streaming-cicd-script/input'
    dataset_to_write = spark_session.read.schema(visit_schema).format('json').load(historical_data_dir)

    (dataset_to_write.write.format('delta').mode('overwrite')
     .save('/tmp/dedp/ch11/11-streaming/02-hybrid-source-apache-spark-structured-streaming-cicd-script/delta_table'))
