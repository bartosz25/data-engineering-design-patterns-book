#!/usr/bin/env python3

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Time, Types, Duration
from pyflink.datastream import WindowedStream, DataStream, CheckpointingMode, ExternalizedCheckpointRetention
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows, CountTrigger, ContinuousEventTimeTrigger

from flink_configuration import prepare_execution_environment
from mapper import map_json_to_visit
from visit_timestamp_assigner import VisitTimestampAssigner
from visit_window_processor import VisitWindowProcessor

env = prepare_execution_environment()

checkpoint_mode = CheckpointingMode.EXACTLY_ONCE
delivery_guarantee = DeliveryGuarantee.EXACTLY_ONCE
# It's here where we configure the checkpoint frequency
checkpoint_interval_30_seconds = 30000
env.enable_checkpointing(checkpoint_interval_30_seconds, mode=checkpoint_mode)
env.get_checkpoint_config().set_externalized_checkpoint_retention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)

kafka_source = (KafkaSource.builder().set_bootstrap_servers('kafka:9092')
    .set_group_id('dedp_visits_reader')
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema()).set_topics('visits').build())

watermark_strategy = (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(5))
                      .with_timestamp_assigner(VisitTimestampAssigner()))
data_source = env.from_source(
    source=kafka_source,
    watermark_strategy=watermark_strategy,
    source_name="Kafka Source"
).assign_timestamps_and_watermarks(watermark_strategy).uid("Kafka Source")


records_per_visit_counts: WindowedStream = (data_source.map(map_json_to_visit)
    .key_by(lambda visit: visit.visit_id).window(TumblingEventTimeWindows.of(Time.minutes(15)))
    .allowed_lateness(0)
    #.trigger(CountTrigger.of(4)))
    .trigger(ContinuousEventTimeTrigger.of(Time.minutes(2))))

window_output: DataStream = records_per_visit_counts.process(VisitWindowProcessor(), Types.STRING()).uid(
    "window output")

kafka_sink = (KafkaSink.builder().set_bootstrap_servers("kafka:9092")
    .set_record_serializer(KafkaRecordSerializationSchema.builder()
                           .set_topic('visits_counter')
                           .set_value_serialization_schema(SimpleStringSchema()).build())
    .set_delivery_guarantee(delivery_guarantee).set_transactional_id_prefix('visits_counter_id_')
    .set_property("transaction.timeout.ms", str(1 * 60 * 1000)).build())

window_output.sink_to(kafka_sink)

env.execute('DEDP reprocessor demo')
