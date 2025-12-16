# Batch reprocessor - stateful with Apache Spark and Delta Lake

1. Start Docker containers:
```
rm -rf /tmp/dedp/ch11/03-data-reprocessing/02-batch-reprocessor-stateful-apache-spark-delta-lake
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [visits_counter_job.py](visits_counter_job.py)
* it's the streaming version of our job; it reads data with the streaming API and uses a shared
business logic defined in [shared_job_logic.py](shared_job_logic.py) to count number of visits for each tumbling window
3. Explain the [data_synchronization_job.py](data_synchronization_job.py)
* this job will be responsible for writing all raw records from the input Kafka topic to a partitioned 
table in Delta Lake
4. Explain the [visits_counter_reprocessing_job.py](visits_counter_reprocessing_job.py)
* it's the batch version of the `visits_counter_job.py` that we are going mostly use in the context
of the reprocessing
* as you can see, it uses a different I/O abstraction but keeps the same business logic by referencing the
transformations from `shraed_job_logic.py`
5. Start the `data_synchronization_job.py`
6. Start the `visits_counter_job.py`
7. Open Apache Kafka producer and send the first batch of records:
```
docker exec -ti dedp_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits

{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T07:00:00.000000", "page": "home"}
{"event_id": "click-2", "visit_id": 1, "event_time": "2025-09-04T07:01:00.000000", "page": "index"}
{"event_id": "click-3", "visit_id": 1, "event_time": "2025-09-04T07:02:00.000000", "page": "page"}
```

Check if the records were correctly written to the table by listing the content of the Delta Lake table:
```
watch ls /tmp/dedp/ch11/03-data-reprocessing/02-batch-reprocessor-stateful-apache-spark-delta-lake/visits_table/_delta_log/
```

If you see...:
```
00000000000000000000.crc
00000000000000000000.json
00000000000000000001.crc
00000000000000000001.json
_staged_commits
```

...add new records to advance the watermark:
```
{"event_id": "click-4", "visit_id": 1, "event_time": "2025-09-04T07:33:00.000000", "page": "home-2"}
{"event_id": "click-5", "visit_id": 1, "event_time": "2025-09-04T07:34:00.000000", "page": "index-2"}
{"event_id": "click-6", "visit_id": 1, "event_time": "2025-09-04T07:35:00.000000", "page": "page-2"}
```

Yet again, check the content and look for these two files:
```
00000000000000000002.crc
00000000000000000002.json
```

8. If the new files are there, open Kafka console consumer to read the windowed counts:
```
docker exec -ti dedp_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits_counter --from-beginning
```

You should see:
```
{"window":{"start":"2025-09-04T07:00:00.000+02:00","end":"2025-09-04T07:10:00.000+02:00"},"count":3}
```

9. Add a bunch of new records that will advance the watermark and generate new output:
```
{"event_id": "click-7", "visit_id": 1, "event_time": "2025-09-04T08:06:00.000000", "page": "home-3"}
{"event_id": "click-8", "visit_id": 1, "event_time": "2025-09-04T08:07:00.000000", "page": "index-3"}
{"event_id": "click-9", "visit_id": 1, "event_time": "2025-09-04T08:08:00.000000", "page": "page-3"}
```

The Kafka consumer should print the new closed window:
```
{"window":{"start":"2025-09-04T07:30:00.000+02:00","end":"2025-09-04T07:40:00.000+02:00"},"count":3}
```

While the Delta Lake log should record a new commit:
```
00000000000000000003.crc
00000000000000000003.json
```

10. Let's assume now we got some late data that is going to be ignored by the streaming job by the watermark. 
```
{"event_id": "click-300", "visit_id": 1, "event_time": "2025-09-04T07:03:00.000000", "page": "page"}
{"event_id": "click-301", "visit_id": 1, "event_time": "2025-09-04T07:04:00.000000", "page": "page"}
```

Before moving on, wait for the new commit file in the Delta log:
```
00000000000000000004.crc
00000000000000000004.json
```


11. Now, let's assume we want to reprocess the first generated window without interrupting the streaming job. Run the 
```
python visits_counter_reprocessing_job.py "2025-09-04T07:00:00.000" "2025-09-04T07:10:00.000"
```


11. The job should reprocess the rows and write them to the Apache Kafka topic. You should see the reprocessed
records in the console:
```
{"window":{"start":"2025-09-04T07:00:00.000+02:00","end":"2025-09-04T07:10:00.000+02:00"},"count":5}
```