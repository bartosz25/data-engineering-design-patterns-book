# Data ordering - FIFO with Apache Spark and Apache Kafka

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```

## Individual delivery
1. Explain the [fifo_orderer_individual_send_job.py](fifo_orderer_individual_send_job.py)
* that's the first FIFO producer that under-the-hood calls the [kafka_writer_individual_send.py](kafka_writer_individual_send.py)
  * the `KafkaWriterIndividualSend` sends one record at once and waits for it to be delivered before resuming
    the delivery
  * ⚠️ Of course, as stated in the book, this producer does guarantee the order only within the execution
       If for whatever reason it has to retry, the same record will be sent again.
2. Run the `fifo_orderer_individual_send_job.py`
3. Consume the data from the output topic:
```
docker exec dedp_ch05_fifo_kafka kafka-console-consumer.sh --topic visits-individual-send --bootstrap-server localhost:9092 --property print.timestamp=true  --property print.partition=true  

CreateTime:1709350696711	Partition:0	{"visit_id": "139806183209856_10", "event_time": "2024-01-01T01:46:00+00:00", "user_id": "139806183209856_461f37f0-5471-441c-8520-42df08441667", "keep_private": false, "page": "category_15", "context": {"referral": "Twitter", "ad_id": "ad 1", "user": {"ip": "106.102.136.20", "login": "randy03", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "20.1", "network_type": "LAN", "device_type": "MacBook", "device_version": "3.0"}}}
CreateTime:1709350697158	Partition:0	{"visit_id": "139806183209856_0", "event_time": "2024-01-01T04:52:00+00:00", "user_id": "139806183209856_be6313b0-8c47-4bcf-8749-a0ecb347b937", "keep_private": false, "page": "main", "context": {"referral": null, "ad_id": null, "user": {"ip": "105.52.71.34", "login": "timothy50", "connected_since": "2023-12-18T00:00:00+00:00"}, "technical": {"browser": "Firefox", "browser_version": "20.2", "network_type": "Wi-Fi", "device_type": "Smartphone", "device_version": "3.0"}}}
CreateTime:1709350698715	Partition:0	{"visit_id": "139806183209856_4", "event_time": "2024-01-01T05:02:13+00:00", "user_id": "139806183209856_62fba090-0cf0-4a5f-8f37-46a363b5be34", "keep_private": false, "page": "contact", "context": {"referral": "LinkedIn", "ad_id": "ad 1", "user": {"ip": "69.8.218.189", "login": "carolynreid", "connected_since": "2023-12-08T00:00:00+00:00"}, "technical": {"browser": "Firefox", "browser_version": "23.1", "network_type": "5G", "device_type": "Smartphone", "device_version": "2.0"}}}
```

The timestamp printed in the beginning of each record should be separated by 1 second interval for each partition.

## Batched delivery without concurrency
1. Explain the [fifo_orderer_bulk_send_job.py](fifo_orderer_bulk_send_job.py)
* that's the second implementation possible; here a producer still batches the records but never sends 
  more than one batch at a time. 
  * the guarantee comes from the `max.in.flight.requests.per.connection` property set to 1
2. Run the `fifo_orderer_bulk_send_job.py`
3. Consume the data from the output topic:
```
docker exec dedp_ch05_fifo_kafka kafka-console-consumer.sh --topic visits-batched --bootstrap-server localhost:9092  --property print.timestamp=true  --property print.partition=true  

CreateTime:1709351555677	Partition:1	{"visit_id": "139806183209856_3", "event_time": "2024-01-01T04:16:00+00:00", "user_id": "139806183209856_b3a919c0-49b9-464e-9249-4203b4b5ff08", "keep_private": false, "page": "about", "context": {"referral": "YouTube", "ad_id": "ad 3", "user": {"ip": "90.75.102.193", "login": "robertsbridget", "connected_since": "2023-12-08T00:00:00+00:00"}, "technical": {"browser": "Chrome", "browser_version": "21.0", "network_type": "4G", "device_type": "PC", "device_version": "5.0"}}}
CreateTime:1709351555677	Partition:1	{"visit_id": "139806183209856_4", "event_time": "2024-01-01T09:49:00+00:00", "user_id": "139806183209856_ebeb2fe0-6f00-45bc-b011-d1080d480e10", "keep_private": true, "page": "home", "context": {"referral": "Google Search", "ad_id": "ad 2", "user": {"ip": "203.216.50.2", "login": "pterry", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "23.0", "network_type": "Wi-Fi", "device_type": "PC", "device_version": "5.0"}}}
CreateTime:1709351555677	Partition:1	{"visit_id": "139806183209856_5", "event_time": "2024-01-01T02:05:00+00:00", "user_id": "139806183209856_1ea6f575-3e29-4c3d-9f99-fa925a1196cb", "keep_private": false, "page": "main", "context": {"referral": "Google Search", "ad_id": "ad 4", "user": {"ip": "143.10.222.126", "login": "chadsanders", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "23.0", "network_type": "Wi-Fi", "device_type": "iPad", "device_version": "2.0"}}}
CreateTime:1709351555677	Partition:1	{"visit_id": "139806183209856_6", "event_time": "2024-01-01T02:06:00+00:00", "user_id": "139806183209856_0cfc516e-4fda-4afe-b0f0-86f7e210447f", "keep_private": false, "page": "categories", "context": {"referral": "Facebook", "ad_id": null, "user": {"ip": "85.174.14.32", "login": "rpacheco", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "23.1", "network_type": "LAN", "device_type": "Smartphone", "device_version": "3.0"}}}
CreateTime:1709351555677	Partition:1	{"visit_id": "139806183209856_7", "event_time": "2024-01-01T10:43:57+00:00", "user_id": "139806183209856_2f0fb19c-605f-4e00-8975-391fbf3957a1", "keep_private": false, "page": "category_14", "context": {"referral": null, "ad_id": null, "user": {"ip": "142.224.112.93", "login": "angelasteele", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "23.2", "network_type": "4G", "device_type": "PC", "device_version": "6.0"}}}
```

You can see that the append time is the same for all the records.

## Batched with idempotent producer
1. Explain the [fifo_orderer_bulk_idempotent_job.py](fifo_orderer_bulk_idempotent_job.py)
* here the job relies on an _idempotent Kafka producer_, i.e. a producer that internally uses a sequence number
  to guarantee that the records are written as a consecutive sequence of events
  * it accepts up to 5 in-flight requests per connection; 5 is the number of requests supported by the
    broker that guarantees the ordering
2. Run the `fifo_orderer_bulk_idempotent_job.py`
3. Consume the data from the output topic:
```
docker exec dedp_ch05_fifo_kafka kafka-console-consumer.sh --topic visits-idempotent --bootstrap-server localhost:9092  --property print.timestamp=true  --property print.partition=true  

CreateTime:1709352125682	Partition:1	{"visit_id": "139806183209856_0", "event_time": "2024-01-01T10:47:01+00:00", "user_id": "139806183209856_ee6f03ba-53d3-4b64-a1cf-2cac81fce44f", "keep_private": false, "page": "home", "context": {"referral": "Google Ads", "ad_id": "ad 4", "user": {"ip": "162.197.172.77", "login": "cooperjesse", "connected_since": "2023-12-12T00:00:00+00:00"}, "technical": {"browser": "Firefox", "browser_version": "23.0", "network_type": "5G", "device_type": "iPhone", "device_version": "6.0"}}}
CreateTime:1709352125682	Partition:1	{"visit_id": "139806183209856_10", "event_time": "2024-01-01T11:17:00+00:00", "user_id": "139806183209856_b3a919c0-49b9-464e-9249-4203b4b5ff08", "keep_private": false, "page": "category_19", "context": {"referral": "YouTube", "ad_id": "ad 3", "user": {"ip": "90.75.102.193", "login": "robertsbridget", "connected_since": "2023-12-08T00:00:00+00:00"}, "technical": {"browser": "Chrome", "browser_version": "21.0", "network_type": "4G", "device_type": "PC", "device_version": "5.0"}}}
CreateTime:1709352125682	Partition:1	{"visit_id": "139806183209856_2", "event_time": "2024-01-01T05:53:21+00:00", "user_id": "139806183209856_623f1990-0b8d-433e-a369-00d9f85f3a08", "keep_private": false, "page": "home", "context": {"referral": null, "ad_id": "ad 5", "user": {"ip": "132.27.232.96", "login": "fritzalexis", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "25.09", "network_type": "4G", "device_type": "MacBook", "device_version": "4.0"}}}
CreateTime:1709352125682	Partition:1	{"visit_id": "139806183209856_5", "event_time": "2024-01-01T09:44:00+00:00", "user_id": "139806183209856_1ea6f575-3e29-4c3d-9f99-fa925a1196cb", "keep_private": false, "page": "home", "context": {"referral": "Google Search", "ad_id": "ad 4", "user": {"ip": "143.10.222.126", "login": "chadsanders", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "23.0", "network_type": "Wi-Fi", "device_type": "iPad", "device_version": "2.0"}}}
CreateTime:1709352125683	Partition:1	{"visit_id": "139806183209856_7", "event_time": "2024-01-01T09:14:00+00:00", "user_id": "139806183209856_0cfc516e-4fda-4afe-b0f0-86f7e210447f", "keep_private": false, "page": "main", "context": {"referral": "Facebook", "ad_id": null, "user": {"ip": "85.174.14.32", "login": "rpacheco", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "23.1", "network_type": "LAN", "device_type": "Smartphone", "device_version": "3.0"}}}
CreateTime:1709352125683	Partition:1	{"visit_id": "139806183209856_8", "event_time": "2024-01-01T03:59:00+00:00", "user_id": "139806183209856_1df7fee4-d852-4b8a-b94c-ffc7ff4d176d", "keep_private": false, "page": "index", "context": {"referral": null, "ad_id": "ad 4", "user": {"ip": "15.212.121.198", "login": "timothyrich", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "24.11", "network_type": "4G", "device_type": "Smartphone", "device_version": "1.0"}}}
CreateTime:1709352125683	Partition:1	{"visit_id": "139806183209856_9", "event_time": "2024-01-01T06:30:16+00:00", "user_id": "139806183209856_0498e8f6-3234-40f8-8182-a39f51df678e", "keep_private": false, "page": "about", "context": {"referral": "Facebook", "ad_id": null, "user": {"ip": "113.161.132.222", "login": "martinmoreno", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "20.1", "network_type": "4G", "device_type": "MacBook", "device_version": "6.0"}}}
```

This time you can see that the timestamps are pretty close to each other meaning that they're result of buffering
multiple batches and delivering them within the max in-flight requests limit. 
