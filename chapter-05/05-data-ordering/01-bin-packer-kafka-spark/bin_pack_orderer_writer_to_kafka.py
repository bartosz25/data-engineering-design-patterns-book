from typing import Iterator, Optional

from confluent_kafka import Producer
from pyspark import Row


def write_records_to_kafka(bootstrap_server: str, output_topic: str, visits_rows: Iterator[Row]):
    producer = Producer({
        'bootstrap.servers': bootstrap_server,
        'queue.buffering.max.ms': 5000
    })
    delivery_groups = []
    groups_index = 0
    last_visit_id: Optional[str] = None
    for visit in visits_rows:
        if visit.visit_id != last_visit_id:
            last_visit_id = visit.visit_id
            groups_index = 0
        if len(delivery_groups) <= groups_index:
            delivery_groups.append([])
        delivery_groups[groups_index].append(visit)
        groups_index += 1

    batch = 1
    for group_to_send in delivery_groups:
        for visit_in_group in group_to_send:
            producer.produce(
                topic=output_topic, key=visit_in_group.visit_id, value=visit_in_group.value,
                headers=[('delivery_batch', str(batch))]
            )
        batch += 1
        producer.flush()
