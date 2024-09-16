# Data ordering - bin-packer with Apache Spark and AWS Kinesis Data Streams

1. Start Localstack:
```
LAMBDA_DOCKER_NETWORK=host
localstack start -d
```

2. Create Kinesis Data Streams:
```
awslocal kinesis create-stream --stream-name visits-ordered --shard-count 2
```

3. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```

4. Explain the [bin_pack_orderer_job.py](bin_pack_orderer_job.py)
* the job performs the bin-ordering in two parts:
  * first, in the `write_sorted_events` it sorts all records by visit_id and event_time, so that the bins creator
    can simply create groups
  * second, the `write_records_to_kafka` function creates delivery groups by iterating the incoming rows and putting
    events for a given visit in different delivery bins
5. Run the `bin_pack_orderer_job.py`


6. Run `kinesis_reader.py` to validate the order. You should see:
```
-------------- Validating shardId-000000000000 -----------------
-------------- Validating shardId-000000000001 -----------------
```

7. Let's see the disordering now. Clear the previous checkpoint location:
```
rm -rf /tmp/dedp/ch05/05-data-ordering/01-bin-packer-kinesis-spark/checkpoint-bin-pack-orderer
```
8. Restart the `bin_pack_orderer_job.py`
9. Stop the job after few minutes and run the `kinesis_reader.py`. You should see some errors:
```
-------------- Validating shardId-000000000000 -----------------
Event time error detected for 139866064382848_3 // 2024-01-01T00:00:00+00:00 not greater or equal to 2024-01-01T04:46:00+00:00
Event time error detected for 139866064382848_4 // 2024-01-01T00:00:00+00:00 not greater or equal to 2024-01-01T04:37:00+00:00
Event time error detected for 139866064382848_6 // 2024-01-01T00:00:00+00:00 not greater or equal to 2024-01-01T04:28:00+00:00
Event time error detected for 139866064382848_8 // 2024-01-01T00:00:00+00:00 not greater or equal to 2024-01-01T04:19:00+00:00
-------------- Validating shardId-000000000001 -----------------
Event time error detected for 139866064382848_0 // 2024-01-01T00:00:00+00:00 not greater or equal to 2024-01-01T03:56:15+00:00
Event time error detected for 139866064382848_1 // 2024-01-01T00:00:00+00:00 not greater or equal to 2024-01-01T04:41:00+00:00
Event time error detected for 139866064382848_2 // 2024-01-01T00:00:00+00:00 not greater or equal to 2024-01-01T04:35:00+00:00
Event time error detected for 139866064382848_5 // 2024-01-01T00:00:00+00:00 not greater or equal to 2024-01-01T04:30:27+00:00
Event time error detected for 139866064382848_7 // 2024-01-01T00:00:00+00:00 not greater or equal to 2024-01-01T04:23:00+00:00
Event time error detected for 139866064382848_9 // 2024-01-01T00:00:00+00:00 not greater or equal to 2024-01-01T04:36:22+00:00

```
The issue is related to the replayed dataset that at some point breaks the ordering. It's only a proof that the ordering 
detection works as expected.

10. Stop the Docker container and the localstack:
```
localstack stop
```