import os
import time

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Time, Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream import WindowedStream, DataStream, OutputTag
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows, ContinuousEventTimeTrigger, EventTimeTrigger

from flink_configuration import prepare_execution_environment
from mapper import map_json_to_visit
from visit_timestamp_assigner import VisitTimestampAssigner
from visit_window_processor import VisitWindowProcessor

os.environ['TZ'] = 'UTC'
time.tzset()

env = prepare_execution_environment()

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
    .key_by(lambda visit: visit.visit_id).window(TumblingEventTimeWindows.of(Time.minutes(15)))
    .allowed_lateness(0)
    .trigger(EventTimeTrigger()))

pages_stats: OutputTag = OutputTag('pages_stats', Types.STRING())

window_output: DataStream = records_per_letter_count.process(VisitWindowProcessor(pages_stats), Types.STRING()).uid(
    'window output')

kafka_sink_counter = (KafkaSink.builder().set_bootstrap_servers("localhost:9094")
    .set_record_serializer(KafkaRecordSerializationSchema.builder()
                           .set_topic('visits_counter')
                           .set_value_serialization_schema(SimpleStringSchema()).build())
              .build())

window_output.sink_to(kafka_sink_counter)


kafka_sink_aggregates = (KafkaSink.builder().set_bootstrap_servers("localhost:9094")
    .set_record_serializer(KafkaRecordSerializationSchema.builder()
                           .set_topic('visited_pages')
                           .set_value_serialization_schema(SimpleStringSchema()).build())
              .build())
side_output_stream = window_output.get_side_output(pages_stats)
side_output_stream.sink_to(kafka_sink_aggregates)


env.execute()