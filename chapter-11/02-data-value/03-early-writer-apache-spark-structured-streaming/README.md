# Early writer - Apache Spark Structured Streaming

1. Explain the [incremental_approach_job.py](incremental_approach_job.py)
2. Explain the [incremental_approach_transformation.py](incremental_approach_transformation.py)
3. Prepare the checkpoint directory:
```
cd dataset
rm -rf /tmp/dedp/ch11/11-streaming/02-early-writer-apache-spark-structured-streaming
mkdir -p /tmp/dedp/ch11/11-streaming/02-early-writer-apache-spark-structured-streaming/checkpoint
```
4. Start Apache Kafka broker:
```
cd docker
docker-compose down --volumes; docker-compose up
```
5. Start `incremental_approach_job.py`

6. Open Apache Kafka producer:
```
docker exec -ti dedp_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits
```

and produce some visits manually, and wait each time for the running job to process each row:
```
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T07:00:00.000000"}
{"event_id": "click-2", "visit_id": 1, "event_time": "2025-09-04T07:01:00.000000"}
{"event_id": "click-3", "visit_id": 1, "event_time": "2025-09-04T07:02:00.000000"}
{"event_id": "click-4", "visit_id": 1, "event_time": "2025-09-04T07:00:00.000000"}
{"event_id": "click-1", "visit_id": 2, "event_time": "2025-09-04T07:10:00.000000"}

```

You should see the visits counter incrementing each time and emitting the updated value downstream:
```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
|1       |1            |false      |
+--------+-------------+-----------+

-------------------------------------------
Batch: 1
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
+--------+-------------+-----------+

-------------------------------------------
Batch: 2
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
|1       |2            |false      |
+--------+-------------+-----------+

-------------------------------------------
Batch: 3
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
+--------+-------------+-----------+

-------------------------------------------
Batch: 4
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
|1       |3            |false      |
+--------+-------------+-----------+

-------------------------------------------
Batch: 5
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
+--------+-------------+-----------+ 

-------------------------------------------
Batch: 6
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
|1       |4            |false      |
+--------+-------------+-----------+

-------------------------------------------
Batch: 7
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
|2       |1            |false      |
+--------+-------------+-----------+

-------------------------------------------
Batch: 8
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
+--------+-------------+-----------+

-------------------------------------------
Batch: 9
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
|1       |5            |false      |
+--------+-------------+-----------+

```

7. When you want to stop the demo, send an event from the console producer to close all pending states:
```
{"event_id": "stop", "visit_id": 1, "event_time": "2026-12-29T07:49:15.189575"}
```
I'm using here a date far in the future for the sake of simplicity.
8. You should see the job printing the sessions with the `is_finished` flag set to `True`:
```


-------------------------------------------
Batch: 10
-------------------------------------------
+--------+-------------+-----------+
|visit_id|visited_pages|is_finished|
+--------+-------------+-----------+
|2       |1            |true       |
|1       |5            |true       |
+--------+-------------+-----------+
```
9. Stop the job.
