#!/usr/bin/env python3
import os

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, OutputTag, DataStream
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink

from kafka_sink import create_kafka_sink
from visit_mapper import MapJsonToReducedVisit

# This configuration is very important
# Without it, you may encounter class loading-related errors, such as:
# Caused by: java.lang.ClassCastException: cannot assign instance of
#   org.apache.kafka.clients.consumer.OffsetResetStrategy to field
#   org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer.offsetResetStrategy
#   of type org.apache.kafka.clients.consumer.OffsetResetStrategy
#   in instance of org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer
config = Configuration()
config.set_string("classloader.resolve-order", "parent-first")
env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
# The parallelism must be the same as the number of input partitions
# Otherwise the watermark won't advance
env.set_parallelism(2)
env.configure(config)
env.add_jars(
    f"file://{os.getcwd()}/kafka-clients-3.2.3.jar",
    f"file://{os.getcwd()}/flink-connector-base-1.17.0.jar",
    f"file://{os.getcwd()}/flink-connector-kafka-1.17.0.jar"
)

kafka_source = KafkaSource.builder().set_bootstrap_servers('localhost:9094') \
    .set_group_id('visits_reader') \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()).set_topics('visits').build()

watermark_strategy = WatermarkStrategy.no_watermarks()

data_source = env.from_source(
    source=kafka_source,
    watermark_strategy=watermark_strategy,
    source_name="Visits Topic"
).uid("Visits Topic").assign_timestamps_and_watermarks(watermark_strategy)

invalid_data_output: OutputTag = OutputTag('invalid_visits', Types.STRING())

visits: DataStream = data_source.map(MapJsonToReducedVisit(invalid_data_output), Types.STRING())

kafka_sink_valid_data: KafkaSink = create_kafka_sink('localhost:9094', 'visits_reduced')

kafka_sink_invalid_data: KafkaSink = create_kafka_sink('localhost:9094', 'visits_dlq')

visits.get_side_output(invalid_data_output).sink_to(kafka_sink_invalid_data)
visits.sink_to(kafka_sink_valid_data)

env.execute('Reduced visits generator')
