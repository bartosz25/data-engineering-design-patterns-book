from confluent_kafka import avro

from producer_factory import create_avro_producer

if __name__ == '__main__':
    kafka_avro_producer = create_avro_producer(avro.load('schemas/v2_visit_without_visit_id.avsc'))

    for visit_id in range(0, 20):
        kafka_avro_producer.produce(topic='visits_forward', value={
            'event_time': 1722830073000+visit_id
        })

    kafka_avro_producer.flush()
