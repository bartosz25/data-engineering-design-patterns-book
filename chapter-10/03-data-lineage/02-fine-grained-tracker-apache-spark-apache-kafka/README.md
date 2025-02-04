# Fine-grained tracker  - row-level lineage with Apache Kafka and Apache Spark Structured Streaming

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```
2. Explain the [visits_decorator_job.py](visits_decorator_job.py)
* the job writes each row with an extra lineage information added as a header
  * the lineage information includes job version, job name, and the micro-batch number

3. Explain the [visits_reducer_job.py](visits_reducer_job.py)
* the job performs the same lineage decoration action as the `visits_decorator_job.py`; the lineage includes
the same information as well
* besides the lineage information about itself, the job also includes the lineage information about its 
upstream dependency, i.e. data producer of the processed record
  * this information might be useful for you to reach out to your data producer in case of any issues with all
    required details; otherwise, you would need to rely on a global dataset lineage without the details about the job 
    version or job name producing given record

3. Start the `visits_decorator_job.py`
4. Start the `visits_reducer_job.py`

5. Run a console consumer on top of the decorated visits topic:
```
docker exec dedp_kafka kafka-console-consumer.sh --topic visits-decorated --bootstrap-server localhost:9092 --property print.headers=true
```

You should see the records with metadata attributes printed in the beginning: 
```
job_version:v1.0.0,job_name:visits_decorator,batch_version:8	{"visit_id": "139694144162688_9", "event_time": "2024-01-01T00:48:00+00:00", "user_id": "139694144162688_2d28ad41-a635-44ab-9d40-c65b1955447c", "keep_private": false, "page": "category_2", "context": {"referral": "Twitter", "ad_id": "ad 3", "user": {"ip": "111.90.140.97", "login": "sanchezchristian", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "25.09", "network_type": "LAN", "device_type": "iPhone", "device_version": "2.0"}}}
job_version:v1.0.0,job_name:visits_decorator,batch_version:8	{"visit_id": "139694144162688_8", "event_time": "2024-01-01T00:34:00+00:00", "user_id": "139694144162688_30bd65a6-95b0-4171-bda5-bf11ee01ceb8", "keep_private": true, "page": "category_4", "context": {"referral": null, "ad_id": "ad 5", "user": {"ip": "216.74.45.232", "login": "sarah11", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "22.0", "network_type": "4G", "device_type": "iPhone", "device_version": "5.0"}}}
job_version:v1.0.0,job_name:visits_decorator,batch_version:8	{"visit_id": "139694144162688_4", "event_time": "2024-01-01T00:49:00+00:00", "user_id": "139694144162688_33d18b01-4602-40eb-a2bd-1015699e4c74", "keep_private": true, "page": "page_1", "context": {"referral": "YouTube", "ad_id": "ad 4", "user": {"ip": "139.164.240.22", "login": "smiller", "connected_since": "2023-12-31T00:00:00+00:00"}, "technical": {"browser": "Firefox", "browser_version": "24.11", "network_type": "5G", "device_type": "iPad", "device_version": "5.0"}}}
job_version:v1.0.0,job_name:visits_decorator,batch_version:8	{"visit_id": "139694144162688_5", "event_time": "2024-01-01T00:49:00+00:00", "user_id": "139694144162688_ec81c8a3-1e3f-4230-bd97-9c49c1e45da3", "keep_private": false, "page": "contact", "context": {"referral": "Google Search", "ad_id": "ad 1", "user": {"ip": "16.173.3.183", "login": "swagner", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "23.2", "network_type": "LAN", "device_type": "MacBook", "device_version": "1.0"}}}
job_version:v1.0.0,job_name:visits_decorator,batch_version:8	{"visit_id": "139694144162688_6", "event_time": "2024-01-01T00:50:00+00:00", "user_id": "139694144162688_8df13bf9-5912-41fe-ba2e-c5d33f84b0e8", "keep_private": false, "page": "categories", "context": {"referral": "Facebook", "ad_id": "ad 5", "user": {"ip": "161.85.199.77", "login": "royclark", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "22.0", "network_type": "5G", "device_type": "PC", "device_version": "6.0"}}}
job_version:v1.0.0,job_name:visits_decorator,batch_version:8	{"visit_id": "139694144162688_7", "event_time": "2024-01-01T00:39:00+00:00", "user_id": "139694144162688_31ff36cd-53e7-4d0c-bdba-4285a0aa5612", "keep_private": false, "page": "home", "context": {"referral": null, "ad_id": "ad 5", "user": {"ip": "126.9.46.189", "login": "kelleykristen", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "23.2", "network_type": "Wi-Fi", "device_type": "PC", "device_version": "4.0"}}}
job_version:v1.0.0,job_name:visits_decorator,batch_version:9	{"visit_id": "139694144162688_4", "event_time": "2024-01-01T00:50:00+00:00", "user_id": "139694144162688_33d18b01-4602-40eb-a2bd-1015699e4c74", "keep_private": true, "page": "index", "context": {"referral": "YouTube", "ad_id": "ad 4", "user": {"ip": "139.164.240.22", "login": "smiller", "connected_since": "2023-12-31T00:00:00+00:00"}, "technical": {"browser": "Firefox", "browser_version": "24.11", "network_type": "5G", "device_type": "iPad", "device_version": "5.0"}}}
job_version:v1.0.0,job_name:visits_decorator,batch_version:9	{"visit_id": "139694144162688_0", "event_time": "2024-01-01T00:55:00+00:00", "user_id": "139694144162688_1eadb074-3769-4a52-b5c0-de7f880630fd", "keep_private": false, "page": "categories", "context": {"referral": "YouTube", "ad_id": "ad 3", "user": {"ip": "47.201.143.212", "login": "justin21", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "23.1", "network_type": "4G", "device_type": "MacBook", "device_version": "2.0"}}}
job_version:v1.0.0,job_name:visits_decorator,batch_version:9	{"visit_id": "139694144162688_5", "event_time": "2024-01-01T00:51:00+00:00", "user_id": "139694144162688_ec81c8a3-1e3f-4230-bd97-9c49c1e45da3", "keep_private": false, "page": "contact", "context": {"referral": "Google Search", "ad_id": "ad 1", "user": {"ip": "16.173.3.183", "login": "swagner", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "23.2", "network_type": "LAN", "device_type": "MacBook", "device_version": "1.0"}}}
job_version:v1.0.0,job_name:visits_decorator,batch_version:9	{"visit_id": "139694144162688_1", "event_time": "2024-01-01T00:50:00+00:00", "user_id": "139694144162688_5e5b5a8f-34a7-44de-a100-5a347ce8bd9b", "keep_private": true, "page": "page_15", "context": {"referral": null, "ad_id": "ad 1", "user": {"ip": "54.142.48.4", "login": "vasquezjennifer", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "23.2", "network_type": "5G", "device_type": "Smartphone", "device_version": "3.0"}}}
```

6. Run a console consumer for the reduced visits topic:
```
docker exec dedp_kafka kafka-console-consumer.sh --topic visits-reduced --bootstrap-server localhost:9092 --property print.headers=true
```

You should see the records with metadata attributes printed in the beginning: 
```
job_name:visits_reducer,job_version:v1.0.0,batch_version:15,parent_lineage:["{job_version, v1.0.0}","{job_name, visits_decorator}","{batch_version, 11}"]	{"visit_id":"139694144162688_2","event_time":"2024-01-01T01:47:00.000+01:00","user_id":"139694144162688_5691baca-b688-453f-852d-f2a25d909f6e","page":"index"}
job_name:visits_reducer,job_version:v1.0.0,batch_version:15,parent_lineage:["{job_version, v1.0.0}","{job_name, visits_decorator}","{batch_version, 11}"]	{"visit_id":"139694144162688_6","event_time":"2024-01-01T02:01:00.000+01:00","user_id":"139694144162688_8df13bf9-5912-41fe-ba2e-c5d33f84b0e8","page":"category_17"}
job_name:visits_reducer,job_version:v1.0.0,batch_version:15,parent_lineage:["{job_version, v1.0.0}","{job_name, visits_decorator}","{batch_version, 11}"]	{"visit_id":"139694144162688_3","event_time":"2024-01-01T01:54:00.000+01:00","user_id":"139694144162688_f3ef1428-160c-4cb6-b8fb-13b4b2ed878b","page":"category_13"}
job_name:visits_reducer,job_version:v1.0.0,batch_version:15,parent_lineage:["{job_version, v1.0.0}","{job_name, visits_decorator}","{batch_version, 11}"]	{"visit_id":"139694144162688_7","event_time":"2024-01-01T01:47:00.000+01:00","user_id":"139694144162688_31ff36cd-53e7-4d0c-bdba-4285a0aa5612","page":"about"}
job_name:visits_reducer,job_version:v1.0.0,batch_version:15,parent_lineage:["{job_version, v1.0.0}","{job_name, visits_decorator}","{batch_version, 11}"]	{"visit_id":"139694144162688_8","event_time":"2024-01-01T01:39:00.000+01:00","user_id":"139694144162688_30bd65a6-95b0-4171-bda5-bf11ee01ceb8","page":"contact"}
job_name:visits_reducer,job_version:v1.0.0,batch_version:16,parent_lineage:["{job_version, v1.0.0}","{job_name, visits_decorator}","{batch_version, 12}"]	{"visit_id":"139694144162688_5","event_time":"2024-01-01T02:04:00.000+01:00","user_id":"139694144162688_ec81c8a3-1e3f-4230-bd97-9c49c1e45da3","page":"home"}
job_name:visits_reducer,job_version:v1.0.0,batch_version:16,parent_lineage:["{job_version, v1.0.0}","{job_name, visits_decorator}","{batch_version, 12}"]	{"visit_id":"139694144162688_4","event_time":"2024-01-01T01:56:00.000+01:00","user_id":"139694144162688_33d18b01-4602-40eb-a2bd-1015699e4c74","page":"main"}
job_name:visits_reducer,job_version:v1.0.0,batch_version:16,parent_lineage:["{job_version, v1.0.0}","{job_name, visits_decorator}","{batch_version, 12}"]	{"visit_id":"139694144162688_6","event_time":"2024-01-01T02:02:00.000+01:00","user_id":"139694144162688_8df13bf9-5912-41fe-ba2e-c5d33f84b0e8","page":"home"}
```