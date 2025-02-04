from avro.schema import Schema
from confluent_kafka.avro import AvroProducer


def tested_topic() -> str:
    return 'visits'


def create_avro_producer(value_schema: Schema) -> AvroProducer:
    return AvroProducer({
        'bootstrap.servers': 'localhost:9092',
        'schema.registry.url': 'http://localhost:8081',
        # the auto registry is required to see the runtime failures for the incompatible schemas
        # however, it may not be the best configuration for the production workloads where you can
        # prefer the schema validation instead
        'schema.registry.auto.register.schemas': True
    }, default_value_schema=value_schema)
