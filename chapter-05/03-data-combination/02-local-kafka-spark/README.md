# Data combination - local Spark with Kafka partitions

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [recent_visit_info_preparator_job.py](recent_visit_info_preparator_job.py)
* the job generates partial aggregated visits
  * it's partial because the outcome comes from the records loaded in each micro-batch
* the logic consists of sorting all records in each task (`sortWithinPartitions`) and of 
  iterating these sorted results to build a partial visit 
* the delivery happens in the `foreachPartition` with the help of a custom Apache Kafka producer
  * we don't use here the native Kafka sink as it can't be easily integrated with PySpark API
3. Start the `recent_visit_info_preparator_job`
4. Run a console consumer on top of the aggregated visits topic:
```
docker exec dedp_ch05_combiner_kafka kafka-console-consumer.sh --topic visits-aggregated --bootstrap-server localhost:9092
```

After some seconds you should see the first records coming in:
```
{"visit_id": "140119021427584_0", "pages": ["about", "page_10"], "saw_ad": false, "network_types": ["LAN"]}
{"visit_id": "140119021427584_1", "pages": ["index", "main"], "saw_ad": true, "network_types": ["4G"]}
{"visit_id": "140119021427584_2", "pages": ["about", "main"], "saw_ad": true, "network_types": ["5G"]}
{"visit_id": "140119021427584_3", "pages": ["main", "about"], "saw_ad": true, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_8", "pages": ["categories", "category_3"], "saw_ad": false, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_4", "pages": ["contact", "page_13"], "saw_ad": true, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_6", "pages": ["index", "categories"], "saw_ad": true, "network_types": ["4G"]}
{"visit_id": "140119021427584_5", "pages": ["page_4", "contact"], "saw_ad": true, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_4", "pages": ["page_12", "about"], "saw_ad": true, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_6", "pages": ["main", "categories"], "saw_ad": true, "network_types": ["4G"]}
{"visit_id": "140119021427584_0", "pages": ["contact", "index"], "saw_ad": false, "network_types": ["LAN"]}
{"visit_id": "140119021427584_1", "pages": ["index", "about"], "saw_ad": true, "network_types": ["4G"]}
{"visit_id": "140119021427584_5", "pages": ["main", "about"], "saw_ad": true, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_2", "pages": ["index", "main"], "saw_ad": true, "network_types": ["5G"]}
{"visit_id": "140119021427584_3", "pages": ["categories", "about"], "saw_ad": true, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_8", "pages": ["contact", "home"], "saw_ad": false, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_0", "pages": ["contact", "about", "main", "category_14"], "saw_ad": false, "network_types": ["LAN"]}
{"visit_id": "140119021427584_1", "pages": ["home", "about", "page_12", "about"], "saw_ad": true, "network_types": ["4G"]}
{"visit_id": "140119021427584_2", "pages": ["contact", "main", "page_2", "contact"], "saw_ad": true, "network_types": ["5G"]}
{"visit_id": "140119021427584_3", "pages": ["main", "page_8", "categories", "categories"], "saw_ad": true, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_8", "pages": ["categories", "main", "about", "about"], "saw_ad": false, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_4", "pages": ["main", "home", "about", "about"], "saw_ad": true, "network_types": ["Wi-Fi"]}
{"visit_id": "140119021427584_6", "pages": ["main", "index", "main", "index"], "saw_ad": true, "network_types": ["4G"]}
{"visit_id": "140119021427584_5", "pages": ["categories", "page_7", "category_17", "home"], "saw_ad": true, "network_types": ["Wi-Fi"]}

```
