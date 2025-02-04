import os
import sys
from datetime import datetime, timedelta

from confluent_kafka import Producer

sys.path.append(f'{os.getcwd()}/protobuf_output/python')

from protobuf_output.python.definitons import visit_pb2, invalid_visit_pb2
from protovalidate import validate, ValidationError

if __name__ == '__main__':
    def visit(event_time: datetime, visit_id='id_1', page='home', user_id='user_1'):
        visit_to_write = visit_pb2.Visit()
        if visit_id is not None:
            visit_to_write.visit_id = visit_id
        visit_to_write.page = page
        if user_id is not None:
            visit_to_write.user_id = user_id
        visit_to_write.event_time.FromDatetime(event_time)
        return visit_to_write
    visit_in_the_past = datetime.utcnow() - timedelta(minutes=10)
    visit_in_the_future = datetime.utcnow() + timedelta(minutes=10)

    kafka_producer = Producer({
        'bootstrap.servers': 'localhost:9094',
    })
    visits_to_send = [
        visit(visit_id='visit1', user_id='u1', page='index', event_time=visit_in_the_past),
        visit(visit_id='visit1', user_id='u1', page='contact', event_time=visit_in_the_past),
        visit(visit_id=None, user_id='u1', page='contact', event_time=visit_in_the_past),
        visit(visit_id='visit3', user_id=None, page='contact', event_time=visit_in_the_past),
        visit(visit_id='visit4', user_id='u3', page='contact.html', event_time=visit_in_the_past),
        visit(visit_id='visit5', user_id='u3', page='contact', event_time=visit_in_the_future)
    ]
    valid_visits_topic = 'valid_visits'
    invalid_visits_topic = 'invalid_visits'
    for visit_to_send in visits_to_send:
        try:
            validate(visit_to_send)
            kafka_producer.produce(
                topic=valid_visits_topic, value=visit_to_send.SerializeToString()
            )
        except ValidationError as e:
            errors = [invalid_visit_pb2.ValidationError(field=error.field_path, message=error.message)
                      for error in e.errors()]
            error_message = invalid_visit_pb2.InvalidVisit(visit=visit_to_send, errors=errors)
            kafka_producer.produce(
                topic=invalid_visits_topic, value=error_message.SerializeToString()
            )
    kafka_producer.flush()