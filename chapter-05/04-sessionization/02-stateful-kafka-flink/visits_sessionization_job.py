#!/usr/bin/env python3
import json
import os

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration, Duration, Types, Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, DataStream, KeySelector, \
    TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.datastream.window import EventTimeSessionWindows

from visit_timestamp_assigner import VisitTimestampAssigner
from visits_processor import VisitToSessionConverter

# This configuration is very important
# Without it, you may encounter class loading-related errors, such as:
# Caused by: java.lang.ClassCastException: cannot assign instance of
#   org.apache.kafka.clients.consumer.OffsetResetStrategy to field
#   org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer.offsetResetStrategy
#   of type org.apache.kafka.clients.consumer.OffsetResetStrategy
#   in instance of org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer
config = Configuration()
config.set_string("classloader.resolve-order", "parent-first")
config.set_string("rest.port", "46167")
env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

# The parallelism must be the same as the number of input partitions
# Otherwise the watermark won't advance
env.set_parallelism(2)
env.configure(config)
env.add_jars(
    f"file://{os.getcwd()}/kafka-clients-3.7.0.jar",
    f"file://{os.getcwd()}/flink-connector-base-1.18.0.jar",
    f"file://{os.getcwd()}/flink-connector-kafka-3.1.0-1.18.jar"
)

kafka_source: KafkaSource = (KafkaSource.builder().set_bootstrap_servers('localhost:9094')
                             .set_group_id('visits_reader')
                             .set_starting_offsets(KafkaOffsetsInitializer.earliest())
                             .set_value_only_deserializer(SimpleStringSchema()).set_topics('visits').build())

watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(20)) \
    .with_timestamp_assigner(VisitTimestampAssigner())

visits_input_data_stream: DataStream = env.from_source(
    source=kafka_source,
    watermark_strategy=watermark_strategy,
    source_name="visits topics"
).uid("kafka-visits-reader").assign_timestamps_and_watermarks(watermark_strategy)


class VisitIdSelector(KeySelector):

    def get_key(self, value: str) -> str:
        visit_json = json.loads(value)
        return visit_json['visit_id']


sessions: DataStream = (visits_input_data_stream
                        .key_by(VisitIdSelector())
                        .window(EventTimeSessionWindows.with_gap(Time.minutes(10)))
                        .allowed_lateness(Time.minutes(15).to_milliseconds())
                        .process(VisitToSessionConverter(), Types.STRING()).uid('sessionizer'))

sessions_kafka_sink: KafkaSink = (KafkaSink.builder().set_bootstrap_servers("localhost:9094")
                                  .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                                         .set_topic('sessions')
                                                         .set_value_serialization_schema(SimpleStringSchema())
                                                         .build())
                                  .build())

sessions.sink_to(sessions_kafka_sink).uid('sessions-kafka-sink')

env.execute('Sessions generator')
