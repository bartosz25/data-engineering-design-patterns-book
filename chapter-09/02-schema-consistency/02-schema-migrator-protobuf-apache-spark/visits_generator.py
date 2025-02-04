import os
import sys
from datetime import datetime, timedelta

from confluent_kafka import Producer

sys.path.append(f'{os.getcwd()}/protobuf_output/python')

from protobuf_output.python.definitons import visit_pb2

if __name__ == '__main__':
    def visit(event_time: datetime, visit_id='id_1', page='home', user_id='user_1'):
        visit_to_write = visit_pb2.Visit()
        visit_to_write.visit_id = visit_id
        visit_to_write.page = page
        visit_to_write.user_id = user_id
        visit_to_write.event_time.FromDatetime(event_time)
        visit_to_write.ip = f'1.1.1.{user_id}'
        visit_to_write.login = f'login {user_id}'
        visit_to_write.is_connected = True
        visit_to_write.from_page = f'origin {user_id} {visit_id}'
        return visit_to_write
    visit_in_the_past = datetime.utcnow() - timedelta(minutes=10)

    kafka_producer = Producer({
        'bootstrap.servers': 'localhost:9094',
    })
    visits_to_send = [
        visit(visit_id='visit1', user_id='u1', page='index', event_time=visit_in_the_past),
        visit(visit_id='visit1', user_id='u1', page='contact', event_time=visit_in_the_past),
        visit(visit_id='visit1', user_id='u1', page='contact', event_time=visit_in_the_past),
        visit(visit_id='visit3', user_id='u2', page='contact', event_time=visit_in_the_past),
        visit(visit_id='visit4', user_id='u3', page='contact.html', event_time=visit_in_the_past),
        visit(visit_id='visit5', user_id='u3', page='contact', event_time=visit_in_the_past)
    ]
    for visit_to_send in visits_to_send:
        kafka_producer.produce(
            topic='visits', value=visit_to_send.SerializeToString()
        )
    kafka_producer.flush()
