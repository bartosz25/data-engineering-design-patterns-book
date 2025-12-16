# Sidecar pattern - Apache Spark Structured Streaming, Apache Kafka, and Delta Lake

1. Start Apache Kafka broker and generate the dataset:
```
rm -rf /tmp/dedp/ch11/02-data-value/02-sidecar-pattern-apache-spark-structured-streaming/ 
mkdir -p /tmp/dedp/ch11/02-data-value/02-sidecar-pattern-apache-spark-structured-streaming/input
cd dataset
docker-compose down --volumes; docker-compose up
```
2. Create the devices Delta Lake table by running the `devices_table_creator.py`
* we rely on a Delta table here because it provides ACID guarantees, i.e. it doesn't require the table writing 
process to be synchronized with the streaming job
  * if we used a JSON file format here, the streaming job would require a synchronization, i.e. whenever new files are
    written, we should stop the job and resume once the writing process succeeds; otherwise, we would risk having
    partially visible dataset
3. Explain the [visits_enricher_job.py](visits_enricher_job.py)
* the job performs the sidecar actions in two places:
  * first, it combines the real-time visits dataset with a static reference master data
  * next, it writes this combination in two places: in a raw _visits_enriched_ table, and in an aggregated
  _devices_stats_ tables
4. Run the `visits_enricher_job.py`
5. Check the results by running `tables_reader.py`. You should see something like:
```
Visits enriched
+-----------------+-------------------+----------------------------------------------------+-------+---------------------------------------------------------------------------------------------------------+------+------------------------------+-------+
|visit_id         |event_time         |user_id                                             |page   |context                                                                                                  |type  |full_name                     |version|
+-----------------+-------------------+----------------------------------------------------+-------+---------------------------------------------------------------------------------------------------------+------+------------------------------+-------+
|125857803623296_4|2023-11-24 04:14:00|125857803623296_200f4a51-8e45-4cab-9c90-80c73421cce7|main   |{Facebook, NULL, {202.20.228.142, patelariana, 2023-10-27 02:00:00}, {Firefox, 21.0, 4G, iphone, iOS 14}}|iphone|APPLE iPhone SE (Black, 64 GB)|iOS 14 |
|125857803623296_4|2023-11-24 04:15:00|125857803623296_200f4a51-8e45-4cab-9c90-80c73421cce7|page_7 |{Facebook, NULL, {202.20.228.142, patelariana, 2023-10-27 02:00:00}, {Firefox, 21.0, 4G, iphone, iOS 14}}|iphone|APPLE iPhone SE (Black, 64 GB)|iOS 14 |
|125857803623296_4|2023-11-24 04:17:00|125857803623296_200f4a51-8e45-4cab-9c90-80c73421cce7|home   |{Facebook, NULL, {202.20.228.142, patelariana, 2023-10-27 02:00:00}, {Firefox, 21.0, 4G, iphone, iOS 14}}|iphone|APPLE iPhone SE (Black, 64 GB)|iOS 14 |
|125857803623296_4|2023-11-24 04:20:00|125857803623296_200f4a51-8e45-4cab-9c90-80c73421cce7|main   |{Facebook, NULL, {202.20.228.142, patelariana, 2023-10-27 02:00:00}, {Firefox, 21.0, 4G, iphone, iOS 14}}|iphone|APPLE iPhone SE (Black, 64 GB)|iOS 14 |
|125857803623296_4|2023-11-24 04:23:00|125857803623296_200f4a51-8e45-4cab-9c90-80c73421cce7|page_11|{Facebook, NULL, {202.20.228.142, patelariana, 2023-10-27 02:00:00}, {Firefox, 21.0, 4G, iphone, iOS 14}}|iphone|APPLE iPhone SE (Black, 64 GB)|iOS 14 |
+-----------------+-------------------+----------------------------------------------------+-------+---------------------------------------------------------------------------------------------------------+------+------------------------------+-------+
only showing top 5 rows

Devices stats
+------+----------+-----+--------+
|type  |version   |count|batch_id|
+------+----------+-----+--------+
|galaxy|Android 11|12   |1       |
|galaxy|Android 13|12   |1       |
|htc   |Android 14|12   |1       |
|iphone|iOS 13    |12   |1       |
|iphone|iOS 14    |12   |1       |
+------+----------+-----+--------+
```