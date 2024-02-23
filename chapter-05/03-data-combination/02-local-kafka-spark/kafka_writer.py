import json
from typing import Optional

from kafka import KafkaProducer


class KafkaWriter:

    def __init__(self, bootstrap_server: str, output_topic: str):
        self.producer: KafkaProducer = KafkaProducer(bootstrap_servers=bootstrap_server,
                                                     retries=1000,
                                                     value_serializer=lambda event: json.dumps(event).encode('utf-8'))
        self.output_topic = output_topic
        self.in_flight_visit = {'visit_id': None}

    def process(self, row):
        saw_ad_in_visit = row.context.ad_id is not None
        if row.visit_id != self.in_flight_visit['visit_id']:
            if self.in_flight_visit['visit_id'] is not None:
                self.in_flight_visit['network_types'] = list(self.in_flight_visit['network_types']) # quick-fix for object not serializable issue
                self.producer.send(topic=self.output_topic, value=self.in_flight_visit,
                                   key=self.in_flight_visit['visit_id'].encode('utf-8'))
            self.in_flight_visit = {'visit_id': row.visit_id, 'pages': [], 'saw_ad': saw_ad_in_visit,
                                    'network_types': set()}

        self.in_flight_visit['pages'].append(row.page)
        self.in_flight_visit['network_types'].add(row.context.technical.network_type)
        if not self.in_flight_visit['saw_ad'] and saw_ad_in_visit:
            self.in_flight_visit['saw_ad'] = True

    def close(self):
        self.producer.flush()
        self.producer.close()
