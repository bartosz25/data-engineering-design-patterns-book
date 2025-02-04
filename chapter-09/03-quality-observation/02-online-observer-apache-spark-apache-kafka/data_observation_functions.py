import dataclasses
import datetime
from typing import Dict
from typing import List

import confluent_kafka
from pyspark.accumulators import AccumulatorParam
from pyspark.sql import DataFrame
from ydata_profiling import ProfileReport


@dataclasses.dataclass
class PartitionWithOffset:
    partition: int
    offset: int


class PartitionToMaxOffsetAccumulatorParam(AccumulatorParam):
    def zero(self, default_max: PartitionWithOffset):
        return []

    def addInPlace(self, partitions_with_offsets: List[PartitionWithOffset],
                   new_max_candidate: PartitionWithOffset):
        partitions_with_offsets.append(new_max_candidate)
        return partitions_with_offsets


class LagDetector:

    def __init__(self, last_processed_offsets_per_partition: List[PartitionWithOffset],
                 bootstrap_servers: str, topic: str):
        self.last_processed_offsets_per_partition = last_processed_offsets_per_partition
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

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

    def get_last_offsets_per_partition(self) -> Dict[str, int]:
        last_processed_offsets_mapped = {str(entry.partition): entry.offset for entry in self.last_processed_offsets_per_partition}
        last_available_offsets = self._read_last_available_offsets()
        offsets_lag = {}
        for partition, offset in last_available_offsets.items():
            if partition in last_processed_offsets_mapped:
                lag = offset - last_processed_offsets_mapped[partition]
                offsets_lag[partition] = lag
        return offsets_lag


def generate_profile_html_report(visits_dataframe: DataFrame, batch_version: int):
    profile = ProfileReport(visits_dataframe, title=f"Profiling Report for {batch_version}", minimal=True)

    base_dir = '/tmp/dedp/chapter-09/03-quality-observation/02-online-observer-apache-spark-apache-kafka/profiles'
    profile.to_file(f'{base_dir}/profile_{batch_version}.html')
