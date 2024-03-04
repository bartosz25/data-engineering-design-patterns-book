import json
import logging
from typing import Optional

import requests
from confluent_kafka import Producer


class KafkaWriterWithEnricher:
    BUFFER_THRESHOLD = 100

    def __init__(self, bootstrap_server: str, output_topic: str):
        self.producer: Optional[Producer] = None
        self.partition_id: Optional[int] = None
        self.output_topic = output_topic
        self.bootstrap_server = bootstrap_server
        self.buffered_to_enrich = []
        self.enriched_ips = {}

    def open(self, partition_id, epoch_id):
        self.partition_id = partition_id
        self.producer = Producer({
            'bootstrap.servers': self.bootstrap_server,
            'queue.buffering.max.ms': 5000  # flushes the buffer every 5 seconds
        })
        self.buffered_to_enrich = []
        self.enriched_ips = {}
        return True

    def process(self, row):
        if len(self.buffered_to_enrich) == self.BUFFER_THRESHOLD:
            self._enrich_ips()
            self._flush_records()
        else:
            self.buffered_to_enrich.append(row)

    def close(self, error):
        self._enrich_ips()
        self._flush_records()

    def _enrich_ips(self):
        ips = ','.join(set(visit.ip for visit in self.buffered_to_enrich if visit.ip not in self.enriched_ips))
        fetched_ips = requests.get(f'http://localhost:8080/geolocation/fetch?ips={ips}',
                                   headers={
                                       'Content-Type': 'application/json',
                                       'Charset': 'UTF-8'
                                   })
        if fetched_ips.status_code == 200:
            mapped_ips = json.loads(fetched_ips.content)['mapped']
            self.enriched_ips.update(mapped_ips)
        else:
            logging.error(f"Couldn't et the enrichment data: {fetched_ips.content}")

    def _flush_records(self):
        for visit in self.buffered_to_enrich:
            enriched_country = '{}'
            if visit.ip in self.enriched_ips:
                enriched_country = json.dumps(self.enriched_ips[visit.ip])
            enriched_visit = '{"raw": '+visit.value+'}, "country": '+enriched_country+'}'
            self.producer.produce(
                topic=self.output_topic, key=visit.visit_id, value=enriched_visit
            )
        self.producer.flush()
