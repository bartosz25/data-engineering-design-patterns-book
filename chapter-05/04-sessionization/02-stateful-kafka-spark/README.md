# Sessionization - Apache Spark Structured Streaming with Apache Kafka

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [sessions_generator_job.py](sessions_generator_job.py)
* it starts with a pretty classical part for the reading and preparing the input data
* later, it calls the `applyInPandasWithState` where the session accumulation logic is defined
  * `outputStructType` is the schema for the generated session
  * `stateStructType` is the schema for the state of the accumulated session
3. Explain the [sessions_mapper.py](sessions_mapper.py)
* it's the session generation logic
* it defines here 16 minutes time-out for a session
* the logic itself is a simple accumulation of the visited pages, sorted by the visit time
  * whenever a session expires because it doesn't get new data and the watermark passes by, 
    the function generates the final output
4. Start the `sessions_generator_job.py`
5. Run a console consumer on top of the sessions topic:
```
docker exec dedp_ch05_04_02_kafka kafka-console-consumer.sh --topic sessions --bootstrap-server localhost:9092
```

After some time (watermark of 1 minute) you should see the first sessions coming in:
```
{"visit_id":"140719780965248_0","user_id":"140719780965248_f287ad2c-6d87-4cf1-9e9a-2da3d7837bc5",
"start_time":"2024-01-01T08:18:00.000+01:00","end_time":"2024-01-01T08:48:00.000+01:00",
"visited_pages":[{"page":"index","event_time_as_milliseconds":1704093480000},{"page":"contact","event_time_as_milliseconds":1704093660000},
{"page":"home","event_time_as_milliseconds":1704093720000},{"page":"home","event_time_as_milliseconds":1704093960000},
{"page":"main","event_time_as_milliseconds":1704094020000},{"page":"index","event_time_as_milliseconds":1704094200000},
{"page":"main","event_time_as_milliseconds":1704094260000},{"page":"category_7","event_time_as_milliseconds":1704094500000},
{"page":"home","event_time_as_milliseconds":1704094740000},{"page":"contact","event_time_as_milliseconds":1704094800000},
{"page":"contact","event_time_as_milliseconds":1704094980000},{"page":"contact","event_time_as_milliseconds":1704095280000}],
"duration_in_milliseconds":1800000}

```
