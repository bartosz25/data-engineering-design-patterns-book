import json
from datetime import datetime
from typing import Iterator

from pyspark import Row
from pyspark.sql import DataFrame, functions

from data_observation_functions import LagDetector, PartitionToMaxOffsetAccumulatorParam, PartitionWithOffset, \
    generate_profile_html_report
from validation_results import index_validation_results_to_elastichsearch


def write_to_kafka_with_observer(visits: DataFrame, batch_number: int):
    visits.cache()
    (visits.drop('offset', 'partition').write.format('kafka').option('topic', 'processed_visits')
     .option('kafka.bootstrap.servers', 'localhost:9094').save())

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
    visits_to_analyze = (visits.select(functions.from_json(functions.col('value'), visit_schema).alias('visit'),
                                       'offset', 'partition')
                         .selectExpr('visit.*', 'offset', 'partition')
                         .select('visit_id', 'event_time', 'user_id', 'page', 'context.referral', 'context.ad_id',
                                 'context.user.ip', 'context.user.login', 'context.user.connected_since',
                                 'context.technical.browser', 'context.technical.browser_version',
                                 'context.technical.network_type',
                                 'context.technical.device_type', 'context.technical.device_version',
                                 'offset', 'partition')
                         )

    spark_context = visits_to_analyze.sparkSession.sparkContext
    accumulators = {'event_time': spark_context.accumulator(0),
                    'user_id': spark_context.accumulator(0),
                    'page': spark_context.accumulator(0)}
    all_events_accumulator = spark_context.accumulator(0)
    max_offsets_tracker = spark_context.accumulator([],
                                                    PartitionToMaxOffsetAccumulatorParam())

    def analyze_generated_records(visits_iterator: Iterator[Row]):
        max_local_offset = -1
        current_partition = -1
        for visit_record in visits_iterator:
            if not visit_record.event_time:
                accumulators['event_time'].add(1)
            if not visit_record.user_id:
                accumulators['user_id'].add(1)
            if not visit_record.page:
                accumulators['page'].add(1)

            all_events_accumulator.add(1)

            if visit_record.offset > max_local_offset:
                max_local_offset = visit_record.offset
            current_partition = visit_record.partition

        max_offsets_tracker.add(PartitionWithOffset(partition=current_partition, offset=max_local_offset))

    visits_to_analyze.foreachPartition(analyze_generated_records)

    flatten_tracker = [entry[0] for entry in max_offsets_tracker.value]
    lag_detector = LagDetector(
        last_processed_offsets_per_partition=flatten_tracker,
        bootstrap_servers='localhost:9094',
        topic='visits')
    last_offsets_per_partition = lag_detector.get_last_offsets_per_partition()

    generate_profile_html_report(visits_to_analyze, batch_number)

    observation_dump = {
        '@timestamp': datetime.utcnow().isoformat(),
        'invalid_event_time': accumulators['event_time'].value,
        'invalid_user_id': accumulators['user_id'].value,
        'invalid_page': accumulators['page'].value,
        'all_events': all_events_accumulator.value,
        'lag_per_partition': last_offsets_per_partition
    }
    index_validation_results_to_elastichsearch(observation_dump)

    visits.unpersist()
