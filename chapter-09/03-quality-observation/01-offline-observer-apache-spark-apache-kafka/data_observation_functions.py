import datetime
import json
import os
from collections import deque
from typing import Dict, Optional

import confluent_kafka
from pyspark.sql import DataFrame
from ydata_profiling import ProfileReport


class LagDetector:

    def __init__(self, checkpoint_dir: str, bootstrap_servers: str, topic: str):
        self.checkpoint_dir = checkpoint_dir
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def _read_last_processed_offsets(self) -> Optional[Dict[str, int]]:
        commit_path = f'{self.checkpoint_dir}/commits'
        directories = [int(file_name) for file_name in os.listdir(commit_path)
                       if os.path.isfile(os.path.join(commit_path, file_name)) and file_name.isnumeric()]
        if directories:
            last_commit_full_path = f'{commit_path}/{max(directories)}'
            last_commit_number = os.path.basename(os.path.normpath(last_commit_full_path))
            last_committed_offsets_file = f'{self.checkpoint_dir}/offsets/{last_commit_number}'
            with open(last_committed_offsets_file) as f:
                offsets_info_from_checkpoint = str(deque(f, 1)[0])
                offsets_from_checkpoint_as_dict = json.loads(offsets_info_from_checkpoint)['visits']
                return offsets_from_checkpoint_as_dict

    def _read_last_available_offsets(self) -> Dict[str, int]:
        conf = {'bootstrap.servers': self.bootstrap_servers, 'group.id': datetime.datetime.utcnow().isoformat()}
        consumer = confluent_kafka.Consumer(conf)
        try:
            topic_metadata = consumer.list_topics(self.topic, timeout=10)

            last_offsets = {}
            for partition in topic_metadata.topics[self.topic].partitions:
                min_offsets, max_offset = consumer.get_watermark_offsets(confluent_kafka.TopicPartition(self.topic, partition))
                last_offsets[str(partition)] = max_offset
            return last_offsets
        finally:
            consumer.close()

    def get_last_offsets_per_partition(self) -> Dict[str, int]:
        last_processed_offsets = self._read_last_processed_offsets()
        last_available_offsets = self._read_last_available_offsets()

        offsets_lag = {}
        for partition, offset in last_available_offsets.items():
            lag = offset - last_processed_offsets[partition]
            offsets_lag[partition] = lag
        return offsets_lag


def generate_profile_html_report(visits_dataframe: DataFrame, batch_version: int):
    profile = ProfileReport(visits_dataframe, title=f"Profiling Report for {batch_version}", minimal=True)

    base_dir = '/tmp/dedp/chapter-09/03-quality-observation/01-offline-observer-apache-spark-apache-kafka/profiles'
    profile.to_file(f'{base_dir}/profile_{batch_version}.html')
