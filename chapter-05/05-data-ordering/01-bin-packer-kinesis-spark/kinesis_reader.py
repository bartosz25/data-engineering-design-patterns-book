import json

import localstack_client.session as boto3

if __name__ == '__main__':
    kinesis_client = boto3.client('kinesis')

    shards = ['shardId-000000000000', 'shardId-000000000001']

    for shard_id in shards:
        response = kinesis_client.get_shard_iterator(
            StreamName='visits-ordered',
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'
        )
        shard_iterator = response['ShardIterator']

        print(f'-------------- Validating {shard_id} -----------------')
        previous_sequence_number = ''
        previous_event_time_by_visit_id = {}
        while True:
            response = kinesis_client.get_records(ShardIterator=shard_iterator)
            records = response['Records']

            if not records:
                break

            for record in records:
                data = json.loads(record['Data'].decode('utf-8'))

                visit_id = data['visit_id']
                previous_event_time = ''
                if visit_id in previous_event_time_by_visit_id:
                    previous_event_time = previous_event_time_by_visit_id[visit_id]
                if data['event_time'] <=  previous_event_time:
                    print(f'Event time error detected for {visit_id} // {data["event_time"]} not greater or equal to {previous_event_time}')

                previous_event_time_by_visit_id[visit_id] = data['event_time']
                previous_sequence_number = record['SequenceNumber']

            shard_iterator = response['NextShardIterator']