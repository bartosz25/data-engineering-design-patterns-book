# Early writer - Apache Flink

1. Explain the [visits_counter_job.py](visits_counter_job.py)
* the job counts the number of visited pages in tumbling (not overlapping) windows based on the event time column; 
it emits the partial results continuously, once the watermark passes by `window start time + 10 minutes`
  * the continuous data emission is configured with the `ContinuousEventTimeTrigger.of(Time.minutes(10)))`
  * once the window expires, the continuous trigger will emit the final result
  * the continuous trigger will emit the same result twice: once for the continuous trigger
  interval, and once for the closed window. It can be mitigated with a custom trigger but we want to stay
  simple in this demo.

2. Start Apache Kafka broker:
```
cd dataset
docker-compose down --volumes; docker-compose up
```

3. Start `visits_counter_job.py`

4. Start Apache Kafka producer:
```
docker exec -ti dedp_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits
```

and produce some visits manually, and wait each time for the running job to process each row:
```
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:01:00.000000"}
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:04:00.000000"}
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:06:00.000000"}
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:16:00.000000"}
```
At this point the watermark is of _10:11:00_ which is beyond the early trigger interval of the continuous trigger
(_10:10_). That's why you should see an early result emitted:

```
>> VISIT -1- [2025-09-14T11:10:41.791568], range=[2025-09-04T10:00:00 to 2025-09-04T11:00:00], visits=4, partial=True
```

Add some other records to see the window is continuously updated from now on:
```
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:26:00.000000"}
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-04T10:36:00.000000"}
```

You should see one result emitted for each record:
```
>> VISIT -1- [2025-09-14T11:11:11.840003], range=[2025-09-04T10:00:00 to 2025-09-04T11:00:00], visits=5, partial=True
>> VISIT -1- [2025-09-14T11:11:22.754558], range=[2025-09-04T10:00:00 to 2025-09-04T11:00:00], visits=6, partial=True
```

Finally, add an event that opens the next window to see if the window for _10:00-11:00_ will be emitted with the4
partial flag set to `False`:
```
{"event_id": "click-1", "visit_id": 1, "event_time": "2025-09-05T11:16:00.000000"}
```

Normally, you should see the following output now:
```
>> VISIT -1- [2025-09-14T11:16:08.717750], range=[2025-09-04T10:00:00 to 2025-09-04T11:00:00], visits=6, partial=False
>> VISIT -1- [2025-09-14T11:16:08.717954], range=[2025-09-04T10:00:00 to 2025-09-04T11:00:00], visits=6, partial=False
```
