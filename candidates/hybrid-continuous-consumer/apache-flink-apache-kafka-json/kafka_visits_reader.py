from pyflink.common import SimpleStringSchema, WatermarkStrategy, Duration
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

from mapper import JsonToVisitMapper

import datetime
import json
from typing import Any

from pyflink.common.watermark_strategy import TimestampAssigner

class VisitTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        event = json.loads(value)
        event_time = datetime.datetime.fromisoformat(event['event_time'])
        return int(event_time.timestamp()) * 1000

def create_kafka_visits_reader(env: StreamExecutionEnvironment) -> DataStream:
    visits_input = (KafkaSource
     .builder()
     .set_bootstrap_servers('localhost:9094')
     .set_group_id('visits_reader')
     .set_value_only_deserializer(SimpleStringSchema())
     .set_topics('visits')
     .set_starting_offsets(KafkaOffsetsInitializer.earliest())
     .build())
    watermark_strategy = (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(5))
                          .with_timestamp_assigner(VisitTimestampAssigner()))
    visits_data_source = env.from_source(
        source=visits_input, watermark_strategy=watermark_strategy, source_name='Input Visits from Kafka'
    ).assign_timestamps_and_watermarks(watermark_strategy).uid("Input_Visits_Kafka")

    kafka_visits_mapped = visits_data_source.map(JsonToVisitMapper())

    return kafka_visits_mapped
