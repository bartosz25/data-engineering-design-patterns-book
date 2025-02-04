import datetime
import json
from typing import Dict

import confluent_kafka
from pyspark.sql.streaming.listener import QueryProgressEvent, QueryIdleEvent, QueryStartedEvent, QueryTerminatedEvent, \
    StreamingQueryListener
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

class BatchCompletionSlaListener(StreamingQueryListener):

    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def onQueryProgress(self, event: "QueryProgressEvent") -> None:
        try:
            latest_offsets_per_partition = self._read_last_available_offsets()

            visits_end_offsets = json.loads(event.progress.sources[0].endOffset)
            visits_offsets_per_partition: Dict[str,int] = visits_end_offsets['visits']
            registry = CollectorRegistry()
            metrics_gauge = Gauge('visits_reader_lag',
                                  'Per-partition lag for the visits reader', registry=registry,
                                  labelnames=['partition'])
            for partition, value in visits_offsets_per_partition.items():
                lag = latest_offsets_per_partition[partition] - value
                metrics_gauge.labels(partition=partition).set(lag)

            push_to_gateway('localhost:9091', job='visits_to_delta_synchronizer_lag', registry=registry)
        except Exception as error:
            print(f'An error occurred for sending the gauge: {error}')

    def _read_last_available_offsets(self) -> Dict[str, int]:
        conf = {'bootstrap.servers': self.bootstrap_servers, 'group.id': datetime.datetime.utcnow().isoformat()}
        consumer = confluent_kafka.Consumer(conf)
        try:
            topic_metadata = consumer.list_topics(self.topic, timeout=10)

            last_offsets = {}
            for partition in topic_metadata.topics[self.topic].partitions:
                min_offsets, max_offset = consumer.get_watermark_offsets(
                    confluent_kafka.TopicPartition(self.topic, partition))
                last_offsets[str(partition)] = max_offset
            return last_offsets
        finally:
            consumer.close()

    def onQueryIdle(self, event: "QueryIdleEvent") -> None:
        pass

    def onQueryStarted(self, event: "QueryStartedEvent") -> None:
        pass

    def onQueryTerminated(self, event: "QueryTerminatedEvent") -> None:
        pass