#!/usr/bin/env python3
import os

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, DataStream
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema

from mappers import map_json_to_reduced_visit

# This configuration is very important
# Without it, you may encounter class loading-related errors, such as:
# Caused by: java.lang.ClassCastException: cannot assign instance of
#   org.apache.kafka.clients.consumer.OffsetResetStrategy to field
#   org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer.offsetResetStrategy
#   of type org.apache.kafka.clients.consumer.OffsetResetStrategy
#   in instance of org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer
config = Configuration()
config.set_string('classloader.resolve-order', 'parent-first')

# You can get some extra metrics by enabling Prometheus Gateway. 
# Even though we're not going to use them in the demo, I'm leaving it so that you can see if you find something interesting
#config.set_string('metrics.reporter.prom.factory.class:', 'org.apache.flink.metrics.prometheus.PrometheusReporterFactory')
config.set_string('metrics.reporter.promgateway.factory.class', 'org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterFactory')
config.set_string('metrics.reporter.promgateway.hostUrl', 'http://localhost:9091')
config.set_string('metrics.reporter.promgateway.jobName', 'reduced_visits_synchronizer')
config.set_string('metrics.reporter.promgateway.randomJobNameSuffix', 'false')
config.set_string('metrics.reporter.promgateway.deleteOnShutdown', 'true')
config.set_string('metrics.reporter.promgateway.groupingKey', 'job_version=v1')
config.set_string('metrics.reporter.promgateway.interval', '30 SECONDS')

config.set_string('rest.port', '1111')
env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(2)
env.configure(config)
env.add_jars(
    f'file://{os.getcwd()}/kafka-clients-3.7.0.jar',
    f'file://{os.getcwd()}/flink-connector-base-1.18.0.jar',
    f'file://{os.getcwd()}/flink-metrics-prometheus-1.18.0.jar',
    f'file://{os.getcwd()}/flink-connector-kafka-3.1.0-1.18.jar'
)
env.get_config().set_latency_tracking_interval(15000) # 15 sec

kafka_source: KafkaSource = (KafkaSource.builder().set_bootstrap_servers('localhost:9094')
                             .set_group_id('visits_reader')
                             .set_starting_offsets(KafkaOffsetsInitializer.earliest())
                             .set_value_only_deserializer(SimpleStringSchema()).set_topics('visits').build())

watermark_strategy = WatermarkStrategy.no_watermarks()
kafka_data_stream: DataStream = env.from_source(
    source=kafka_source, watermark_strategy=watermark_strategy, source_name='visits topics'
).uid('kafka-visits-reader').assign_timestamps_and_watermarks(watermark_strategy).uid('kafka_source')

visits: DataStream = (kafka_data_stream.map(map_json_to_reduced_visit, Types.STRING()))

kafka_sink_valid_data: KafkaSink = (KafkaSink.builder().set_bootstrap_servers('localhost:9094')
                                    .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                                           .set_topic('reduced_visits')
                                                           .set_value_serialization_schema(SimpleStringSchema())
                                                           .build())
                                    .build())

visits.sink_to(kafka_sink_valid_data).uid('kafka_sink')

env.execute('Reduced visits data synchronizer')
