# Batch reprocessor - stateful session window with Apache Spark and Delta Lake

1. Start Docker containers:
```
rm -rf /tmp/dedp/ch11/03-data-reprocessing/02-batch-reprocessor-stateful-session-window-apache-spark-delta-lake
mkdir -p /tmp/dedp/ch11/03-data-reprocessing/02-batch-reprocessor-stateful-session-window-apache-spark-delta-lake
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [create_delta_lake_table.py](create_delta_lake_table.py):
* the job creates a Delta Lake table with the records that we would usually synchronize from Apache Kafka
* for the sake of simplicity of this demo, we're going to skip the data synchronization job and assume the records 
have been correctly synchronized

3. Run the `create_delta_lake_table.py`

4. Explain the [reprocessing_data_preparator.py](reprocessing_data_preparator.py)
* the class here prepares the data for reprocessing; it fetches:
  * users to reprocess falling within the reprocessing period
  * visits to reprocess for the reprocessed users from the past and future windows, as long as the visits
    don't break the session inactivity rule

5. Explain the [reprocessing_data_fetchers.py](reprocessing_data_fetchers.py)
* that's the place where the queries are really implemented
  * users are a simple `SELECT ... FROM ... WHERE ... BETWEEN ...` query with the boundaries set to the reprocessing window
  * past visits are built from a query running on global window per partition; as long as there are visits belonging
    to open session, the subsequent queries will run
    * same applies to the future visits retrieval

> [!NOTE]  
> The number of iterations could be reduced to a static number if you allowed some slight inconsistencies. For example,
> if you allowed the static number of iterations to 10 but the 11th partition would also contain some valid visits,
> you would get incomplete session. On another hand, your processing logic and execution time would be simplified.

6. Explain the [sessions_reprocessing_job.py](sessions_reprocessing_job.py)
* that's the entrypoint for the reprocessing job that interacts with the `reprocessing_data_preparator.py`
* it uses the same `transformWithStateInPandas` operation as the real-time version; the single difference is that it doesn't
  persist the state
* besides, the job writes reprocessed records with a dedicated _source_ header in Apache Kafka

7. Run the reprocessing:
```
python sessions_reprocessing_job.py 2025-09-04T10:00:00.000 2025-09-04T11:00:00.000 reprocessing_run_1
```

8. Check the results in Apache Kafka topic:
```
docker exec -ti dedp_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits_counter --from-beginning --property print.headers=true
```

You should see 4 sessions submitted from the reprocessing job:
```
source:reprocessing,run_id:reprocessing_run_1   {"visit_id":"20250904_105200-3","visited_pages":7,"is_finished":true}
source:reprocessing,run_id:reprocessing_run_1   {"visit_id":"20250904_085900-1","visited_pages":18,"is_finished":true}
source:reprocessing,run_id:reprocessing_run_1   {"visit_id":"20250904_100000-2","visited_pages":2,"is_finished":true}
source:reprocessing,run_id:reprocessing_run_1   {"visit_id":"20250904_104500-2","visited_pages":2,"is_finished":true}
```

9. To see the stateful processor is shared with the real-time job, start the real-time job:
```
python visits_duration_calculation_job.py
```

And start Kafka producer:
```
docker exec -ti dedp_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits
```

10. And produce the following records:
```
{"event_id": "click-1", "user_id": 1, "event_time": "2025-10-04T07:00:00.000000", "page": "home"}
{"event_id": "click-2", "user_id": 1, "event_time": "2025-10-04T07:01:00.000000", "page": "index"}
{"event_id": "click-3", "user_id": 1, "event_time": "2025-10-04T07:02:00.000000", "page": "page"}
{"event_id": "click-4", "user_id": 2, "event_time": "2025-10-04T07:02:00.000000", "page": "page"}
{"event_id": "click-5", "user_id": 2, "event_time": "2025-10-04T07:03:00.000000", "page": "page-2"}
```

11. You should see new records in the Kafka topic:
```
NO_HEADERS      {"visit_id":"20251004_070200-2","visited_pages":2,"is_finished":false}
NO_HEADERS      {"visit_id":"20251004_070000-1","visited_pages":3,"is_finished":false}
```

Add some other records to see the stateful operaiton working despite the if condition added to the stateful processor:
```
{"event_id": "click-6", "user_id": 1, "event_time": "2025-10-04T07:05:00.000000", "page": "page-4"}
{"event_id": "click-7", "user_id": 2, "event_time": "2025-10-04T07:06:00.000000", "page": "page-6"}
```

And on the consumer side you should get:
```
NO_HEADERS      {"visit_id":"20251004_070000-1","visited_pages":4,"is_finished":false}
NO_HEADERS      {"visit_id":"20251004_070200-2","visited_pages":3,"is_finished":false}
```
