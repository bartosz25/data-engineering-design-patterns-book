import json
import sys
from datetime import datetime
from typing import Iterator

from click.core import batch
from pyspark import Row
from pyspark.sql import SparkSession, DataFrame, functions

from data_observation_functions import LagDetector, generate_profile_html_report
from validation_results import index_validation_results_to_elastichsearch

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[2]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = (spark.readStream
                         .option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('subscribe', 'processed_visits')
                         .option('startingOffsets', 'EARLIEST')
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
    visits_to_observe = (input_data_stream
                         .selectExpr('CAST(value AS STRING)')
                         .select(functions.from_json(functions.col('value'), visit_schema).alias('visit'))
                         .selectExpr('visit.*')
                         .select('visit_id', 'event_time', 'user_id', 'page', 'context.referral', 'context.ad_id',
                                 'context.user.ip', 'context.user.login', 'context.user.connected_since',
                                 'context.technical.browser', 'context.technical.browser_version',
                                 'context.technical.network_type',
                                 'context.technical.device_type', 'context.technical.device_version')
                         )


    def generate_and_write_observations(visits: DataFrame, batch_number: int):
        visits.cache()

        visits.createOrReplaceTempView('visits_to_observe')
        results = visits.sparkSession.sql('''
        SELECT
            COUNT(*) AS all_rows,
            SUM(CASE WHEN event_time IS NULL THEN 1 ELSE 0 END) AS invalid_event_time,
            SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS invalid_user_id,
            SUM(CASE WHEN page IS NULL THEN 1 ELSE 0 END) AS invalid_page
        FROM visits_to_observe
        ''').collect()

        generate_profile_html_report(visits, batch_number)

        lag_detector = LagDetector(
            checkpoint_dir='/tmp/dedp/chapter-09/03-quality-observation/01-offline-observer-apache-spark-apache-kafka/passthrough',
            bootstrap_servers='localhost:9094',
            topic='visits')
        last_offsets_per_partition = lag_detector.get_last_offsets_per_partition()

        observation_dump = {
            '@timestamp': datetime.utcnow().isoformat(),
            'invalid_event_time': results[0].invalid_event_time,
            'invalid_user_id': results[0].invalid_user_id,
            'invalid_page': results[0].invalid_page,
            'all_events': results[0].all_rows,
            'lag_per_partition': last_offsets_per_partition
        }
        index_validation_results_to_elastichsearch(observation_dump)

        visits.unpersist(blocking=True)


    query = (visits_to_observe.writeStream.foreachBatch(generate_and_write_observations)
             .option('checkpointLocation',
                     '/tmp/dedp/chapter-09/03-quality-observation/01-offline-observer-apache-spark-apache-kafka/observer').start())

    query.awaitTermination()
