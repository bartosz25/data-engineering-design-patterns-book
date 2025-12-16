# Streaming reprocessor - Apache Spark Structured Streaming

1. Explain [visits_windows_counter_job.py](visits_windows_counter_job.py)
* the job counts visits per tumbling windows of 15 minutes
2. Prepare the checkpoint directory:
```
cd dataset
rm -rf /tmp/dedp/ch11/03-data-reprocessing/01-stream-reprocessor-apache-spark-structured-streaming
mkdir -p /tmp/dedp/ch11/03-data-reprocessing/01-stream-reprocessor-apache-spark-structured-streaming/checkpoint
```
3. Start Apache Kafka broker:
```
docker-compose down --volumes; docker-compose up
```

4. Start `visits_windows_counter_job.py`

5. Open Apache Kafka producer:
```
docker exec -ti dedp_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits
```

and produce some visits manually by taking all the visits at once to the console producer:
* 
```
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T07:00:00.000000"}
{"event_id": "click-2", "visit_id": 1, "event_time": "2025-09-04T07:01:00.000000"}
{"event_id": "click-3", "visit_id": 1, "event_time": "2025-09-04T07:02:00.000000"}
```

Wait for the output to be generated and produce the next batch:
```
{"event_id": "click-4", "visit_id": 1, "event_time": "2025-09-04T07:00:00.000000"}
{"event_id": "click-1", "visit_id": 2, "event_time": "2025-09-04T07:10:00.000000"}
{"event_id": "click-1", "visit_id": 2, "event_time": "2025-09-04T07:20:00.000000"}
```

And finally, produce the last events:
```
{"event_id": "click-1", "visit_id": 2, "event_time": "2025-09-04T07:25:00.000000"}
{"event_id": "click-1", "visit_id": 2, "event_time": "2025-09-04T07:35:00.000000"}
```

6. Stop the `visits_windows_counter_job.py`. At this point you should see:
```
-------------------------------------------
Batch: 0
-------------------------------------------
+------+-----+
|window|count|
+------+-----+
+------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------+-----+
|window                                    |count|
+------------------------------------------+-----+
|{2025-09-04 07:00:00, 2025-09-04 07:10:00}|3    |
+------------------------------------------+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+------+-----+
|window|count|
+------+-----+
+------+-----+

-------------------------------------------
Batch: 3
-------------------------------------------
+------------------------------------------+-----+
|window                                    |count|
+------------------------------------------+-----+
|{2025-09-04 07:00:00, 2025-09-04 07:10:00}|4    |
|{2025-09-04 07:10:00, 2025-09-04 07:20:00}|1    |
|{2025-09-04 07:20:00, 2025-09-04 07:30:00}|1    |
+------------------------------------------+-----+

-------------------------------------------
Batch: 4
-------------------------------------------
+------+-----+
|window|count|
+------+-----+
+------+-----+

-------------------------------------------
Batch: 5
-------------------------------------------
+------------------------------------------+-----+
|window                                    |count|
+------------------------------------------+-----+
|{2025-09-04 07:20:00, 2025-09-04 07:30:00}|2    |
|{2025-09-04 07:30:00, 2025-09-04 07:40:00}|1    |
+------------------------------------------+-----+

-------------------------------------------
Batch: 6
-------------------------------------------
+------+-----+
|window|count|
+------+-----+
+------+-----+
```

7. Check what's in the checkpoint location:
```
tree /tmp/dedp/ch11/03-data-reprocessing/01-stream-reprocessor-apache-spark-structured-streaming/checkpoint -A
/tmp/dedp/ch11/03-data-reprocessing/01-stream-reprocessor-apache-spark-structured-streaming/checkpoint
├── commits
│   ├── 0
│   ├── 1
│   ├── 2
│   ├── 3
│   ├── 4
│   ├── 5
│   └── 6
├── metadata
├── offsets
│   ├── 0
│   ├── 1
│   ├── 2
│   ├── 3
│   ├── 4
│   ├── 5
│   └── 6
├── sources
│   └── 0
│       └── 0
└── state
    └── 0
        ├── 0
        │   ├── 1.zip
        │   ├── 2.zip
        │   ├── 3.zip
        │   ├── 4.zip
        │   ├── 5.zip
        │   ├── 6.zip
        │   ├── 7.zip
        │   ├── _metadata
        │   │   └── schema
        │   └── SSTs
        │       ├── 000012-0024f112-51a8-4db5-bca9-8d7dd366d14f.sst
        │       ├── 000023-e8717cb4-8501-4457-9768-27be0759c2c4.sst
        │       ├── 000034-8f50728d-bc02-433e-b18c-857c4e9ad682.sst
        │       └── 000041-aae9f3c6-19f5-4b15-9389-9454d1ee7fd2.sst
        ├── 1
        │   ├── 1.zip
        │   ├── 2.zip
        │   ├── 3.zip
        │   ├── 4.zip
        │   ├── 5.zip
        │   ├── 6.zip
        │   └── 7.zip
        └── _metadata
            └── metadata
```

```
v1
{"batchWatermarkMs":0,"batchTimestampMs":1757606666015,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.stateStore.encodingFormat":"unsaferow","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"2","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan":"false"}}
{"visits":{"0":0,"1":3}}
```

As you can see, the checkpoint files store the state of the job at each version of the micro-batch. Therefore,
to restore the job to a previous state, we need to invalidate most recent checkpoint files.

8. Restore previous version of the job to the state of the second micro-batch by running `clean_checkpoint_files.py`:
```
python clean_checkpoint_files.py 5
```
9. Restart `visits_windows_counter_job.py`. You should see the last micro-batch reprocessed:
```
-------------------------------------------
Batch: 5
-------------------------------------------
+------------------------------------------+-----+
|window                                    |count|
+------------------------------------------+-----+
|{2025-09-04 07:20:00, 2025-09-04 07:30:00}|2    |
|{2025-09-04 07:30:00, 2025-09-04 07:40:00}|1    |
+------------------------------------------+-----+

-------------------------------------------
Batch: 6
-------------------------------------------
+------+-----+
|window|count|
+------+-----+
+------+-----+
```
