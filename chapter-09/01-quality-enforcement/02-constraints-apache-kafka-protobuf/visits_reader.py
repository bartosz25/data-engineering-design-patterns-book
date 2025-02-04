import os
import sys

from confluent_kafka import Consumer

sys.path.append(f'{os.getcwd()}/protobuf_output/python')

from protobuf_output.python.definitons import visit_pb2, invalid_visit_pb2

if __name__ == '__main__':

    kafka_consumer = Consumer({
        'bootstrap.servers': 'localhost:9094',
        'auto.offset.reset': 'earliest',
        'group.id': 'reader_1'
    })
    kafka_consumer.subscribe(['valid_visits', 'invalid_visits'])
    while True:
        record = kafka_consumer.poll(timeout=5)
        if record:
            value = record.value()
            topic = record.topic()
            if topic == 'valid_visits':
                valid_visit = visit_pb2.Visit()
                valid_visit.ParseFromString(value)
                print('----------- valid visit ---------------')
                print(valid_visit)
            else:
                invalid_visit = invalid_visit_pb2.InvalidVisit()
                invalid_visit.ParseFromString(value)
                print('----------- invalid visit ---------------')
                print(invalid_visit)
            kafka_consumer.commit()
