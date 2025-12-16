# Sidecar pattern - Apache Flink

1. Explain the [visits_processor_job.py](visits_processor_job.py)
* the job counts the number of visited pages in tumbling (not overlapping) windows based on the event time column; 
it emits the partial results continuously, once the watermark passes by `window start time + 10 minutes`
  * the continuous data emission is configured with the `ContinuousEventTimeTrigger.of(Time.minutes(10)))`
  * once the window expires, the continuous trigger will emit the final result
* besides generating counters, the job also emits a secondary - aka side - output with the visited pages in the window
  * to do so, the job uses a _side output_ feature that is used to create a secondary sink in the same job

2. Start Apache Kafka broker:
```
cd docker
docker-compose down --volumes; docker-compose up
```

3. Start `visits_processor_job.py`

4. Start Apache Kafka producer:
```
docker exec -ti dedp_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits
```

and produce some visits manually:
```
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:01:00.000000", "page": "page1"}
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:04:00.000000", "page": "page2"}
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:06:00.000000", "page": "page3"}
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:09:00.000000", "page": "page1"}
```

Add a new event to advance the watermark:
```
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:27:00.000000", "page": "page1"}
```

5. Open Apache Kafka console consumer for the counters topic:
```
docker exec -ti dedp_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits_counter --from-beginning
```

You should see the records written:
```
>> VISIT -1- [2025-09-23T04:42:31.813508], range=[2025-09-04T10:00:00 to 2025-09-04T10:15:00], visits=4
```

6. Open Apache Kafka consuler consumer for the visited pages topic:
```
docker exec -ti dedp_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visited_pages --from-beginning
```

You should see the pages format written from the same job:
```
{"processing_time": "2025-09-23T04:42:31.813508", "window_start": "2025-09-04T10:00:00", "window_end": "2025-09-04T10:15:00", "visited_pages": {"click-1": "page1"}, "visit_id": 1}
```