# Data decoration - Kafka headers

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```
2. Explain the [metadata_decorator_job.py](metadata_decorator_job.py)
* the job uses the `foreachBatch` sink to decorate the incoming rows; for the sake of simplicity, it doesn't
  process anything
* the _headers_ requires an array of struct composed of key string and value binary entries
3. Start the `metadata_decorator_job.py`
4. Run a console consumer on top of the decorated visits topic:
```
docker exec dedp_ch05_decorator_kafka kafka-console-consumer.sh --topic visits-decorated --bootstrap-server localhost:9092 --property print.headers=true
```

You should see the records with metadata attributes printed in the beginning: 
```
job_version:v1.0.3,batch_version:18	{"visit_id": "140221880486784_10", "event_time": "2024-01-01T02:50:08+00:00", "user_id": "140221880486784_e2fb4e1c-b14a-4cd2-8540-7b92abc5f86d", "keep_private": false, "page": "main", "context": {"referral": "YouTube", "ad_id": "ad 3", "user": {"ip": "42.192.93.94", "login": "ffuller", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "23.2", "network_type": "5G", "device_type": "iPad", "device_version": "5.0"}}}
job_version:v1.0.3,batch_version:18	{"visit_id": "140221880486784_9", "event_time": "2024-01-01T05:36:12+00:00", "user_id": "140221880486784_e5593a91-e570-45ad-bc44-acd3bdf4b231", "keep_private": false, "page": "page_18", "context": {"referral": "YouTube", "ad_id": "ad 5", "user": {"ip": "208.0.70.29", "login": "xwhite", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "25.09", "network_type": "4G", "device_type": "MacBook", "device_version": "5.0"}}}
job_version:v1.0.3,batch_version:19	{"visit_id": "140221880486784_5", "event_time": "2024-01-01T04:40:00+00:00", "user_id": "140221880486784_263d2fc4-b6d3-4029-9302-9e6affddf788", "keep_private": false, "page": "contact", "context": {"referral": null, "ad_id": "ad 5", "user": {"ip": "147.23.189.181", "login": "gibsonbethany", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "22.0", "network_type": "LAN", "device_type": "Smartphone", "device_version": "3.0"}}}
job_version:v1.0.3,batch_version:19	{"visit_id": "140221880486784_1", "event_time": "2024-01-01T05:25:00+00:00", "user_id": "140221880486784_8b4d568f-22ae-4e77-b127-bdccf43bb8fb", "keep_private": false, "page": "category_4", "context": {"referral": "Facebook", "ad_id": "ad 5", "user": {"ip": "174.200.119.105", "login": "scottbarry", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "21.0", "network_type": "4G", "device_type": "iPad", "device_version": "5.0"}}}
job_version:v1.0.3,batch_version:19	{"visit_id": "140221880486784_10", "event_time": "2024-01-01T03:36:00+00:00", "user_id": "140221880486784_23247697-a8a3-4a74-9d93-3b81123a9754", "keep_private": true, "page": "contact", "context": {"referral": "Facebook", "ad_id": "ad 2", "user": {"ip": "39.186.177.70", "login": "cgraham", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "23.1", "network_type": "LAN", "device_type": "Smartphone", "device_version": "4.0"}}}
job_version:v1.0.3,batch_version:19	{"visit_id": "140221880486784_3", "event_time": "2024-01-01T05:09:00+00:00", "user_id": "140221880486784_06202d9d-9d99-4cf7-9f43-257f4a1783d8", "keep_private": false, "page": "main", "context": {"referral": "Google Search", "ad_id": "ad 2", "user": {"ip": "201.27.179.19", "login": "kkhan", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "20.1", "network_type": "4G", "device_type": "iPad", "device_version": "6.0"}}}
```
