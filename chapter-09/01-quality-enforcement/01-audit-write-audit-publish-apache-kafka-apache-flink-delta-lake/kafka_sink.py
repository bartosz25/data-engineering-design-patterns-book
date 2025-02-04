from pyflink.common import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema


def create_kafka_sink(broker: str, topic_name: str) -> KafkaSink:
    return (KafkaSink.builder().set_bootstrap_servers(broker)
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic(topic_name)
                                   .set_value_serialization_schema(SimpleStringSchema()).build())
            .build())
