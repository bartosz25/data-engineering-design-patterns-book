import os
import time

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Time, Duration
from pyflink.common.typeinfo import BasicTypeInfo, Types
from pyflink.datastream import WindowedStream, DataStream
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.datastream.connectors.hybrid_source import HybridSource
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows, CountTrigger, ContinuousEventTimeTrigger, PurgingTrigger

from flink_configuration import prepare_execution_environment
from mapper import map_json_to_visit
from triggers import PartialEventTimeWindowTrigger
from visit_timestamp_assigner import VisitTimestampAssigner
from visit_window_processor import VisitWindowProcessor

os.environ['TZ'] = 'UTC'
time.tzset()

env = prepare_execution_environment()
base_dir = '/tmp/dedp/ch11/11-streaming/01-hybrid-source-apache-flink/input'

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

records_per_letter_count: WindowedStream = (visits_data_source.map(map_json_to_visit)
    .key_by(lambda visit: visit.visit_id).window(TumblingEventTimeWindows.of(Time.hours(1)))
    .allowed_lateness(0)
    .trigger(ContinuousEventTimeTrigger.of(Time.minutes(10))))

window_output: DataStream = records_per_letter_count.process(VisitWindowProcessor(), Types.STRING()).uid(
    'window output')

window_output.print()

env.execute()
