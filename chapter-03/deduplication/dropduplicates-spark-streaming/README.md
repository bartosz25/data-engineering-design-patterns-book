# Deduplication - streaming job

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```
The generator produces 100 records each time with 50% of duplicates.
2. Explain the [visits_deduplicator.py](visits_deduplicator.py)
* the job uses a stateful deduplication (`dropDuplicates`); it guarantees a deduplication window of 10 minutes
  (watermark value)
* the job only extracts the necessary columns for the deduplication and stores the rest of the data untouched
3. Start the `visits_deduplicator` and let it running for 2-3 minutes.
4. Stop the `visits_deduplicator` and check for the duplicates by running the `duplicates_checker.py`
```
+--------+----------+-----+
|visit_id|event_time|count|
+--------+----------+-----+
+--------+----------+-----+
```
You shouldn't see any duplicates in the output.