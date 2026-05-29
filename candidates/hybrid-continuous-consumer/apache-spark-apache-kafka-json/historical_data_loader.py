from pyspark.sql import SparkSession

def load_historical_data(spark_session: SparkSession):
    visit_schema = 'visit_id STRING, user_id STRING, page STRING'
    historical_data_dir = '/tmp/dedp/candidates/hybrid-continuous-consumer/apache-spark/input'
    formatted_visits_from_json = (spark_session.readStream.schema(visit_schema)
                        .option('maxFilesPerTrigger', 1)
                        .format('json').load(historical_data_dir))

    return formatted_visits_from_json
