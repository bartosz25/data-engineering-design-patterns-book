# Audit-Write-Audit-Publish (AWAP) - streaming job, element-based audit

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [visits_processor_job.py](visits_processor_job.py)
* the job processes raw visits and performs a validation (aka _audit_) before publishing each of them to the publicly
available topic
* it's an example of the element-based AWAP application since the stream processing relies on continuous events
and we don't consider the dataset as a time- or size-bounded one

3. Start the `visits_processor_job.py`
4. Check the content of the valid visits:
```
$ docker exec -ti dedp_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic valid_visits
{"visit_id": "140359575128960_100", "event_time": "2023-11-01T01:13:00+00:00", "user_id": "140359575128960_62f2d1be-2ada-40db-a723-43602c265579", "keep_private": false, "page": "category_3", "context": {"referral": null, "ad_id": null, "user": {"ip": "144.110.174.175", "login": "bellelizabeth", "connected_since": "2023-10-22T00:00:00+00:00"}, "technical": {"browser": "Chrome", "browser_version": "22.0", "network_type": "Wi-Fi", "device_type": "Smartphone", "device_version": "3.0"}}}
{"visit_id": "140359575128960_18", "event_time": "2023-11-01T01:35:00+00:00", "user_id": "140359575128960_92fd480f-2a80-4b9c-900c-1c9c4d9fa2b3", "keep_private": false, "page": "about", "context": {"referral": "Twitter", "ad_id": null, "user": {"ip": "64.245.123.74", "login": "mendozabenjamin", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "22.0", "network_type": "Wi-Fi", "device_type": "MacBook", "device_version": "5.0"}}}
{"visit_id": "140359575128960_100", "event_time": "2023-11-01T01:07:00+00:00", "user_id": "140359575128960_d472fa2c-8211-472e-9dcc-d5b2cc1548d8", "keep_private": false, "page": "home", "context": {"referral": "YouTube", "ad_id": "ad 4", "user": {"ip": "220.219.89.7", "login": "qnavarro", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "20.1", "network_type": "4G", "device_type": "iPad", "device_version": "2.0"}}}
{"visit_id": "140359575128960_24", "event_time": "2023-11-01T01:27:00+00:00", "user_id": "140359575128960_be221d3d-999d-4bbc-b28f-c273c380e8df", "keep_private": true, "page": "index", "context": {"referral": null, "ad_id": null, "user": {"ip": "138.106.168.172", "login": "jeremy93", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "23.2", "network_type": "4G", "device_type": "PC", "device_version": "6.0"}}}
{"visit_id": "140359575128960_25", "event_time": "2023-11-01T01:27:00+00:00", "user_id": "140359575128960_76c5ae51-4a06-44b5-83a5-552a83d06f09", "keep_private": false, "page": "contact", "context": {"referral": "Google Ads", "ad_id": null, "user": {"ip": "209.160.237.68", "login": "stanleypenny", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "20.2", "network_type": "5G", "device_type": "MacBook", "device_version": "2.0"}}}
{"visit_id": "140359575128960_26", "event_time": "2023-11-01T01:22:00+00:00", "user_id": "140359575128960_c749e2ba-2060-4c12-932d-af741dfac192", "keep_private": false, "page": "home", "context": {"referral": "Google Search", "ad_id": "ad 4", "user": {"ip": "169.155.50.69", "login": "linda98", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "20.2", "network_type": "Wi-Fi", "device_type": "iPad", "device_version": "4.0"}}}
```

...and the invalid ones:
```
$ docker exec -ti dedp_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic invalid_visits
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": "main", "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": "contact", "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": "about", "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": "category_11", "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": null, "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": null, "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": null, "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": null, "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": null, "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": "page_20", "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": null, "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": "home", "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": null, "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": "category_17", "context": null}
{"visit_id": null, "event_time": null, "user_id": null, "keep_private": null, "page": null, "context": null}
```