# Hybrid continuous consumer with Apache Spark, Apache Kafka, and JSON

1. Prepare the historical dataset:
```shell
rm -rf /tmp/dedp/candidates/hybrid-continuous-consumer/apache-spark
mkdir -p /tmp/dedp/candidates/hybrid-continuous-consumer/apache-spark/input
```

2. Explain [hybrid_continuous_consumer_job.py](hybrid_continuous_consumer_job.py)
* the job reads batch and streaming data sources which are, respectively, JSON and Apache Kafka
* it applies throughput limits to JSON data source and processes 1 file in each micro-batch
* the job uses the available now trigger to keep testing under control

3. Start Docker image with Apache Kafka and data generator:
```shell
cd dataset
docker-compose down --volumes; docker-compose up
```

4. Create two files with the data:
```shell
echo '{"visit_id": "batch1", "user_id": "user1", "page": "home.html"}' >> /tmp/dedp/candidates/hybrid-continuous-consumer/apache-spark/input/file1.json
echo '{"visit_id": "batch1", "user_id": "user2", "page": "contact.html"}' >> /tmp/dedp/candidates/hybrid-continuous-consumer/apache-spark/input/file1.json
echo '{"visit_id": "batch2", "user_id": "user1", "page": "basket.html"}' >> /tmp/dedp/candidates/hybrid-continuous-consumer/apache-spark/input/file2.json
echo '{"visit_id": "batch2", "user_id": "user2", "page": "confirmation.html"}' >> /tmp/dedp/candidates/hybrid-continuous-consumer/apache-spark/input/file2.json
```

4. Run the `hybrid_continuous_consumer_job.py`

5. Run the `delta_table_reader.py`. You should see data coming from both historical and real-time data sources in the table:
```
+--------------------+-------------------------------------------------+-----------------+---------+-----------------------+
|visit_id            |user_id                                          |page             |origin   |processing_time        |
+--------------------+-------------------------------------------------+-----------------+---------+-----------------------+
|batch1              |user1                                            |home.html        |batch    |2026-05-29 11:10:42.583|
|batch1              |user2                                            |contact.html     |batch    |2026-05-29 11:10:42.583|
|batch2              |user1                                            |basket.html      |batch    |2026-05-29 11:10:39.492|
|batch2              |user2                                            |confirmation.html|batch    |2026-05-29 11:10:39.492|
|195479274926262080_0|274926262080_a85c6bc6-a280-422a-9166-1fe91ead2df5|category_14      |real_time|2026-05-29 11:10:39.492|
|385814274926262080_1|274926262080_201dec12-365b-412a-baa6-c26ab5bf6036|category_5       |real_time|2026-05-29 11:10:39.492|
|195479274926262080_0|274926262080_a85c6bc6-a280-422a-9166-1fe91ead2df5|about            |real_time|2026-05-29 11:10:39.492|
|385814274926262080_1|274926262080_201dec12-365b-412a-baa6-c26ab5bf6036|category_18      |real_time|2026-05-29 11:10:39.492|
+--------------------+-------------------------------------------------+-----------------+---------+-----------------------+

```
As you can notice, the available now trigger processed both existing files but not in the same micro-batch. 
You can see that by comparing the processing_time column which should be different for both batch data sources.