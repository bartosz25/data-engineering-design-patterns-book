from typing import Iterator, Optional, List, Dict, Any

import localstack_client.session as boto3
from pyspark import Row


def write_records_to_kinesis(output_stream: str, visits_rows: Iterator[Row]):
    producer = boto3.client('kinesis')

    delivery_groups = []
    groups_index = 0
    last_visit_id: Optional[str] = None
    for visit in visits_rows:
        if visit.visit_id != last_visit_id:
            last_visit_id = visit.visit_id
            groups_index = 0
        if len(delivery_groups) <= groups_index:
            delivery_groups.append([])
        delivery_groups[groups_index].append({'Data': visit.value, 'PartitionKey': visit.visit_id})
        groups_index += 1

    def _get_chunk_to_deliver(input_records: List[Dict[str, Any]], max_records: int = 500) -> List[Dict[str, Any]]:
        for i in range(0, len(input_records), max_records):
            yield input_records[i:i + max_records]

    for group_to_send in delivery_groups:
        records_to_deliver = list(_get_chunk_to_deliver(group_to_send, 500))
        while len(records_to_deliver) > 0:
            response = producer.put_records(StreamName=output_stream, Records=group_to_send)
            failed_records = response['FailedRecordCount']
            if failed_records:
                records_to_retry = []
                for index, record in enumerate(group_to_send):
                    if 'ErrorCode' in response['Records'][index]:
                        records_to_retry.append(record)
                records_to_deliver = records_to_retry
            else:
                records_to_deliver = []