#!/usr/bin/env python3
import datetime
import json

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration, Time, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, ExternalizedCheckpointCleanup, \
    WindowedStream, DataStream, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows, CountTrigger

from visit import Visit
from visit_timestamp_assigner import VisitTimestampAssigner
from visit_window_processor import VisitWindowProcessor

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
env.set_parallelism(2)
env.configure(config)

# It's here where we configure the checkpoint frequency
checkpoint_interval_30_seconds = 30000
env.enable_checkpointing(checkpoint_interval_30_seconds, mode=CheckpointingMode.AT_LEAST_ONCE)
(env.get_checkpoint_config()
 .enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION))
# Unlike PySpark, you have to define the JARs explicitly for Flink; as the code runs in a Docker container only,
# we hardcode the paths
env.add_jars(
    f"file:///opt/flink/usrlib/kafka-clients-3.2.3.jar",
    f"file:///opt/flink/usrlib/flink-connector-base-1.17.0.jar",
    f"file:///opt/flink/usrlib/flink-connector-kafka-1.17.0.jar"
)

kafka_source = (KafkaSource.builder().set_bootstrap_servers('kafka:9092')
                .set_group_id('visits_reader')
                .set_starting_offsets(KafkaOffsetsInitializer.earliest())
                .set_value_only_deserializer(SimpleStringSchema()).set_topics('visits').build())

watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(VisitTimestampAssigner())
data_source = env.from_source(
    source=kafka_source,
    watermark_strategy=watermark_strategy,
    source_name="Visits topic"
).uid("Visits topic")


def map_json_to_visit(json_payload: str) -> Visit:
    event = json.loads(json_payload)
    event_time = int(datetime.datetime.fromisoformat(event['event_time']).timestamp())
    return Visit(visit_id=event['visit_id'], page=event['page'], event_time=event_time)


records_per_visit_counts: WindowedStream = (data_source.map(map_json_to_visit)
                                            .key_by(lambda visit: visit.visit_id).window(
    TumblingEventTimeWindows.of(Time.seconds(45)))
                                            .allowed_lateness(0)
                                            .trigger(CountTrigger.of(4)))

# must add the Types.STRING() to avoid serialization issues
window_output: DataStream = records_per_visit_counts.process(VisitWindowProcessor(), Types.STRING()).uid(
    "window output")

kafka_sink = (KafkaSink.builder().set_bootstrap_servers("kafka:9092")
              .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                     .set_topic('visits_windows')
                                     .set_value_serialization_schema(SimpleStringSchema()).build())
              .build())

window_output.sink_to(kafka_sink)

env.execute('Checkpoint example')
