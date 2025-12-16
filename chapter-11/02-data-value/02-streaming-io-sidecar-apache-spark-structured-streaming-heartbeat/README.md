# Sidecar pattern - Apache Spark Structured Streaming, Apache Kafka, and Delta Lake

1. Start Apache Kafka broker and generate the dataset:
```
rm -rf /tmp/dedp/ch11/02-data-value/02-sidecar-pattern-apache-spark-structured-streaming-heartbeat/ 
cd dataset
docker-compose down --volumes; docker-compose up
```
2. Explain the [stateless_visits_job.py](stateless_visits_job.py)
* the job reads visits from the input topic and saves them in a Delta Lake table
* the table is used as a temporary buffer that delays the delivery of the visits to the output topic 
  * the delay leverages the batch numbers and we want to avoid situation where the input topic doesn't
    get new data and cannot progress
3. Open Apache Kafka producer and send the first batch of records:
```
docker exec -ti dedp_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits

{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T07:00:00.000000", "page": "home"}
{"event_id": "click-2", "visit_id": 1, "event_time": "2025-09-04T07:01:00.000000", "page": "index"}
{"event_id": "click-3", "visit_id": 1, "event_time": "2025-09-04T07:02:00.000000", "page": "page"}
```

4. Open Apache Kafka consumer:
```
docker exec -ti dedp_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits_output --from-beginning
```

5. Run the `stateless_visits_job.py`

6. After 2 micro-batches, the consumer should show the records:
```
{"event_id":"click-1","visit_id":1,"event_time":"2025-09-04T07:00:00.000+02:00","page":"home","saving_time":"2025-09-29T05:05:53.026+02:00","flush_number":2,"delivery_time":"2025-09-29T05:06:15.585+02:00","delivery_batch":2}
{"event_id":"click-2","visit_id":1,"event_time":"2025-09-04T07:01:00.000+02:00","page":"index","saving_time":"2025-09-29T05:05:53.026+02:00","flush_number":2,"delivery_time":"2025-09-29T05:06:15.585+02:00","delivery_batch":2}
{"event_id":"click-3","visit_id":1,"event_time":"2025-09-04T07:02:00.000+02:00","page":"page","saving_time":"2025-09-29T05:05:53.026+02:00","flush_number":2,"delivery_time":"2025-09-29T05:06:15.585+02:00","delivery_batch":2}
```