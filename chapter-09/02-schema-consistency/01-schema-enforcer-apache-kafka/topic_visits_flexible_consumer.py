import time

from confluent_kafka.avro import AvroConsumer

if __name__ == '__main__':
    consumer: AvroConsumer = AvroConsumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': f'consumer_test_bde{time.time()}',
        'schema.registry.url': 'http://localhost:8081',
    })

    consumer.subscribe(['visits_flexible'])

    visits_counters = {}

    while True:
        polled_records = consumer.poll(2)
        if polled_records:
            print(f'Got {polled_records.key()} >> {polled_records.value()}')
            visit_id = polled_records.value()['visit_id']
            counter_so_far = 0
            if visit_id in visits_counters:
                counter_so_far = visits_counters[visit_id]
            visits_counters[visit_id] = counter_so_far + 1