# Stream reprocessor - stateless Apache Spark with Delta Lake

1. Start Docker containers:
```
rm -rf /tmp/dedp/ch11/03-data-reprocessing/02-batch-reprocessor-apache-spark-delta-lake
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [visits_converter_streaming_job.py](visits_converter_streaming_job.py)
* it's the streaming version of our job; it reads data with the streaming API and uses a shared
business logic defined in [shared_job_logic.py](shared_job_logic.py) to convert the read rows
3. Explain the [data_synchronization_job.py](data_synchronization_job.py)
* this job will be responsible for writing all raw records from the input Kafka topic to a partitioned 
table in Delta Lake
* it's worth adding, the job also saves the metadata such as partition and offsets for each record so that 
we can better target what need to be reprocessed
4. Explain the [visits_converter_reprocessing_job.py](visits_converter_reprocessing_job.py)
* it's the batch version of the `visits_converter_streaming_job.py` that we are going mostly use in the context
of the reprocessing
* as you can see, it uses a different I/O abstraction but keeps the same business logic by referencing the
transformations from `shraed_job_logic.py`
5. Start the `data_synchronization_job.py`
6. Start the `visits_converter_streaming_job.py`
7. Open Apache Kafka producer and send the first batch of records:
```
docker exec -ti dedp_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits

{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T07:00:00.000000", "page": "home"}
{"event_id": "click-2", "visit_id": 1, "event_time": "2025-09-04T07:01:00.000000", "page": "index"}
{"event_id": "click-3", "visit_id": 1, "event_time": "2025-09-04T07:02:00.000000", "page": "page"}
```

Check if the records were correctly written to the table by listing the content of the Delta Lake table:
```
watch ls /tmp/dedp/ch11/03-data-reprocessing/02-batch-reprocessor-apache-spark-delta-lake/visits_table/_delta_log/
```

If you see...:
```
00000000000000000000.crc
00000000000000000000.json
00000000000000000001.crc
00000000000000000001.json
_staged_commits
```

...add new records:
```
{"event_id": "click-4", "visit_id": 1, "event_time": "2025-09-04T07:03:00.000000", "page": "home-2"}
{"event_id": "click-5", "visit_id": 1, "event_time": "2025-09-04T07:04:00.000000", "page": "index-2"}
{"event_id": "click-6", "visit_id": 1, "event_time": "2025-09-04T07:05:00.000000", "page": "page-2"}
```

Yet again, check the content of the watched directory. You should see:
```
00000000000000000000.crc
00000000000000000000.json
00000000000000000001.crc
00000000000000000001.json
00000000000000000002.crc
00000000000000000002.json
_staged_commits
```

If the `00000000000000000002.json` file appeared, add new bunch of records:
```
{"event_id": "click-7", "visit_id": 1, "event_time": "2025-09-04T07:06:00.000000", "page": "home-3"}
{"event_id": "click-8", "visit_id": 1, "event_time": "2025-09-04T07:07:00.000000", "page": "index-3"}
{"event_id": "click-9", "visit_id": 1, "event_time": "2025-09-04T07:08:00.000000", "page": "page-3"}
```

Ensure the new `00000000000000000003.json` file was written in the commit log before moving to the next step:
```
00000000000000000000.crc
00000000000000000000.json
00000000000000000001.crc
00000000000000000001.json
00000000000000000002.crc
00000000000000000002.json
00000000000000000003.crc
00000000000000000003.json
_staged_commits
```

8. Run `read_table.py` to see all records written so far:
```
+-----------------------------------------------------------------------------------------------------+---------+------+----+-----+---+----+
|value                                                                                                |partition|offset|year|month|day|hour|
+-----------------------------------------------------------------------------------------------------+---------+------+----+-----+---+----+
|{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T07:00:00.000000", "page": "home"}   |0        |0     |2025|9    |4  |7   |
|{"event_id": "click-2", "visit_id": 1, "event_time": "2025-09-04T07:01:00.000000", "page": "index"}  |0        |1     |2025|9    |4  |7   |
|{"event_id": "click-3", "visit_id": 1, "event_time": "2025-09-04T07:02:00.000000", "page": "page"}   |0        |2     |2025|9    |4  |7   |
|{"event_id": "click-4", "visit_id": 1, "event_time": "2025-09-04T07:03:00.000000", "page": "home-2"} |0        |3     |2025|9    |4  |7   |
|{"event_id": "click-5", "visit_id": 1, "event_time": "2025-09-04T07:04:00.000000", "page": "index-2"}|0        |4     |2025|9    |4  |7   |
|{"event_id": "click-6", "visit_id": 1, "event_time": "2025-09-04T07:05:00.000000", "page": "page-2"} |0        |5     |2025|9    |4  |7   |
|{"event_id": "click-7", "visit_id": 1, "event_time": "2025-09-04T07:06:00.000000", "page": "home-3"} |0        |6     |2025|9    |4  |7   |
|{"event_id": "click-8", "visit_id": 1, "event_time": "2025-09-04T07:07:00.000000", "page": "index-3"}|0        |7     |2025|9    |4  |7   |
|{"event_id": "click-9", "visit_id": 1, "event_time": "2025-09-04T07:08:00.000000", "page": "page-3"} |0        |8     |2025|9    |4  |7   |
+-----------------------------------------------------------------------------------------------------+---------+------+----+-----+---+----+
```

9. Run `read_topic.py` to see the output written by the streaming job:
```
+-------------------------------------------------------------------------------------------------------------------------+---------+------+
|value                                                                                                                    |partition|offset|
+-------------------------------------------------------------------------------------------------------------------------+---------+------+
|{"event_identifier":"click-1","visit_identifier":1,"event_time":"2025-09-04T07:00:00.000+02:00","visited_page":"home"}   |0        |0     |
|{"event_identifier":"click-2","visit_identifier":1,"event_time":"2025-09-04T07:01:00.000+02:00","visited_page":"index"}  |0        |1     |
|{"event_identifier":"click-3","visit_identifier":1,"event_time":"2025-09-04T07:02:00.000+02:00","visited_page":"page"}   |0        |2     |
|{"event_identifier":"click-4","visit_identifier":1,"event_time":"2025-09-04T07:03:00.000+02:00","visited_page":"home-2"} |0        |3     |
|{"event_identifier":"click-5","visit_identifier":1,"event_time":"2025-09-04T07:04:00.000+02:00","visited_page":"index-2"}|0        |4     |
|{"event_identifier":"click-6","visit_identifier":1,"event_time":"2025-09-04T07:05:00.000+02:00","visited_page":"page-2"} |0        |5     |
|{"event_identifier":"click-7","visit_identifier":1,"event_time":"2025-09-04T07:06:00.000+02:00","visited_page":"home-3"} |0        |6     |
|{"event_identifier":"click-8","visit_identifier":1,"event_time":"2025-09-04T07:07:00.000+02:00","visited_page":"index-3"}|0        |7     |
|{"event_identifier":"click-9","visit_identifier":1,"event_time":"2025-09-04T07:08:00.000+02:00","visited_page":"page-3"} |0        |8     |
+-------------------------------------------------------------------------------------------------------------------------+---------+------+
```

9. Now, let's assume we want to reprocess the first 3 rows without interrupting the streaming job. Run the 
```
python visits_converter_reprocessing_job.py 0 2
```

> [!NOTE]  
> Our demo is based on a one-partition topic. It'll be rarely the case on production. In that case, you will need
> a more complex input structure to your reprocessing job. You can find a good inspiration for Apache Kafka source
> in Apache Spark Structured Streaming where you can restart the job from arbitrary positions per partition, e.g.
> `{"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}`. Look for the `startingOffsets` option on this page: https://spark.apache.org/docs/4.0.0/streaming/structured-streaming-kafka-integration.html


10. The job should reprocess the rows and write them to the Apache Kafka topic. To see that, read the topic
by executing `read_topic.py`:
```
+-------------------------------------------------------------------------------------------------------------------------+---------+------+
|value                                                                                                                    |partition|offset|
+-------------------------------------------------------------------------------------------------------------------------+---------+------+
|{"event_identifier":"click-1","visit_identifier":1,"event_time":"2025-09-04T07:00:00.000+02:00","visited_page":"home"}   |0        |0     |
|{"event_identifier":"click-2","visit_identifier":1,"event_time":"2025-09-04T07:01:00.000+02:00","visited_page":"index"}  |0        |1     |
|{"event_identifier":"click-3","visit_identifier":1,"event_time":"2025-09-04T07:02:00.000+02:00","visited_page":"page"}   |0        |2     |
|{"event_identifier":"click-4","visit_identifier":1,"event_time":"2025-09-04T07:03:00.000+02:00","visited_page":"home-2"} |0        |3     |
|{"event_identifier":"click-5","visit_identifier":1,"event_time":"2025-09-04T07:04:00.000+02:00","visited_page":"index-2"}|0        |4     |
|{"event_identifier":"click-6","visit_identifier":1,"event_time":"2025-09-04T07:05:00.000+02:00","visited_page":"page-2"} |0        |5     |
|{"event_identifier":"click-7","visit_identifier":1,"event_time":"2025-09-04T07:06:00.000+02:00","visited_page":"home-3"} |0        |6     |
|{"event_identifier":"click-8","visit_identifier":1,"event_time":"2025-09-04T07:07:00.000+02:00","visited_page":"index-3"}|0        |7     |
|{"event_identifier":"click-9","visit_identifier":1,"event_time":"2025-09-04T07:08:00.000+02:00","visited_page":"page-3"} |0        |8     |
|{"event_identifier":"click-1","visit_identifier":1,"event_time":"2025-09-04T07:00:00.000+02:00","visited_page":"home"}   |0        |9     |
|{"event_identifier":"click-2","visit_identifier":1,"event_time":"2025-09-04T07:01:00.000+02:00","visited_page":"index"}  |0        |10    |
|{"event_identifier":"click-3","visit_identifier":1,"event_time":"2025-09-04T07:02:00.000+02:00","visited_page":"page"}   |0        |11    |
+-------------------------------------------------------------------------------------------------------------------------+---------+------+
```