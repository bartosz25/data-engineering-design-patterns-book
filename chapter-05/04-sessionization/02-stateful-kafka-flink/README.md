# Sessionization - Apache Flink with session windows 

1. Explain the [visits_sessionization_job.py](visits_sessionization_job.py) 
* the job starts by reading Apache Kafka visits topic and extracting the timestamp used for the watermark 
calculation `VisitTimestampAssigner`
* later, the job defines the session window logic with a grouping key (`key_by`) and a session window with inactivity time
* finally, the job defines the output logic (`VisitToSessionConverter`) with the data sink configuration

2. Start Apache Kafka with the data generator:
```
cd docker
docker-compose down --volumes --remove-orphans; docker-compose up
```
3. Start the `visits_sessionization_job.py` 
4. Start Kafka consumers that is going to read the generated sessions:
`docker exec -ti dedp_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sessions`

After few minutes, you should see the generated sessions flowing in:
```
{"visit_id": "140083900046208_35", "start_time": "2024-06-01T00:00:00+00:00", "end_time": "2024-06-01T01:10:00+00:00", "visited_pages": [[{"page": "category_14", "event_time": "2024-06-01T00:00:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:02:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:06:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:09:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:11:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:13:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:17:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:21:00+00:00"}, {"page": "category_14", "event_time": "2024-06-01T00:25:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:28:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:29:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:32:00+00:00"}, {"page": "index", "event_time": "2024-06-01T00:35:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:38:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:40:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:41:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:46:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:47:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:51:00+00:00"}, {"page": "page_5", "event_time": "2024-06-01T00:53:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:54:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:59:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T01:01:00+00:00"}, {"page": "main", "event_time": "2024-06-01T01:03:00+00:00"}, {"page": "about", "event_time": "2024-06-01T01:05:00+00:00"}, {"page": "main", "event_time": "2024-06-01T01:06:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T01:07:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T01:10:00+00:00"}]], "user_id": ["140083900046208_13f216ce-d336-4da6-aaa6-f1f8f3a3086c"], "data_processing_context": {"generation_time": "2024-08-14T03:03:36.015324", "watermark": "2024-06-01T01:19:59.999000+00:00"}}
{"visit_id": "140083900046208_13", "start_time": "2024-06-01T00:00:00+00:00", "end_time": "2024-06-01T01:11:25+00:00", "visited_pages": [[{"page": "index", "event_time": "2024-06-01T00:00:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:02:00+00:00"}, {"page": "index", "event_time": "2024-06-01T00:03:00+00:00"}, {"page": "category_5", "event_time": "2024-06-01T00:08:00+00:00"}, {"page": "page_6", "event_time": "2024-06-01T00:10:00+00:00"}, {"page": "page_17", "event_time": "2024-06-01T00:15:00+00:00"}, {"page": "category_4", "event_time": "2024-06-01T00:16:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:21:00+00:00"}, {"page": "page_3", "event_time": "2024-06-01T00:23:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:24:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:25:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:26:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:31:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:33:00+00:00"}, {"page": "category_9", "event_time": "2024-06-01T00:35:00+00:00"}, {"page": "category_10", "event_time": "2024-06-01T00:38:00+00:00"}, {"page": "page_6", "event_time": "2024-06-01T00:42:00+00:00"}, {"page": "index", "event_time": "2024-06-01T00:46:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:47:00+00:00"}, {"page": "index", "event_time": "2024-06-01T00:48:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:53:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:54:00+00:00"}, {"page": "category_5", "event_time": "2024-06-01T00:59:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T01:00:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T01:05:00+00:00"}, {"page": "about", "event_time": "2024-06-01T01:08:00+00:00"}, {"page": "home", "event_time": "2024-06-01T01:11:00+00:00"}, {"page": "about", "event_time": "2024-06-01T01:11:25+00:00"}]], "user_id": ["140083900046208_97584539-c3a9-4054-b0b9-713d2ec8bd0f"], "data_processing_context": {"generation_time": "2024-08-14T03:03:36.026742", "watermark": "2024-06-01T01:21:59.999000+00:00"}}
{"visit_id": "140083900046208_6", "start_time": "2024-06-01T01:12:04+00:00", "end_time": "2024-06-01T01:12:04+00:00", "visited_pages": [[{"page": "home", "event_time": "2024-06-01T01:12:04+00:00"}]], "user_id": ["140083900046208_9b27b9db-b26b-4e30-be12-0b6f25e567b7"], "data_processing_context": {"generation_time": "2024-08-14T03:03:41.035179", "watermark": "2024-06-01T01:24:59.999000+00:00"}}
{"visit_id": "140083900046208_19", "start_time": "2024-06-01T00:00:00+00:00", "end_time": "2024-06-01T01:17:00+00:00", "visited_pages": [[{"page": "category_12", "event_time": "2024-06-01T00:00:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:03:00+00:00"}, {"page": "category_15", "event_time": "2024-06-01T00:04:00+00:00"}, {"page": "index", "event_time": "2024-06-01T00:05:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:08:00+00:00"}, {"page": "page_3", "event_time": "2024-06-01T00:10:00+00:00"}, {"page": "page_1", "event_time": "2024-06-01T00:12:00+00:00"}, {"page": "index", "event_time": "2024-06-01T00:17:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:20:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:21:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:24:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:26:00+00:00"}, {"page": "page_5", "event_time": "2024-06-01T00:27:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:32:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:35:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:36:00+00:00"}, {"page": "category_16", "event_time": "2024-06-01T00:41:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:43:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:45:00+00:00"}, {"page": "index", "event_time": "2024-06-01T00:49:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:51:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:53:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:58:00+00:00"}, {"page": "home", "event_time": "2024-06-01T01:00:00+00:00"}, {"page": "home", "event_time": "2024-06-01T01:03:00+00:00"}, {"page": "main", "event_time": "2024-06-01T01:08:00+00:00"}, {"page": "page_18", "event_time": "2024-06-01T01:12:00+00:00"}, {"page": "home", "event_time": "2024-06-01T01:14:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T01:16:00+00:00"}, {"page": "page_7", "event_time": "2024-06-01T01:17:00+00:00"}]], "user_id": ["140083900046208_a0fb7361-ffa3-42be-8576-983dde7e9aa8"], "data_processing_context": {"generation_time": "2024-08-14T03:03:43.977475", "watermark": "2024-06-01T01:26:59.999000+00:00"}}
{"visit_id": "140083900046208_22", "start_time": "2024-06-01T01:22:00+00:00", "end_time": "2024-06-01T01:22:00+00:00", "visited_pages": [[{"page": "page_16", "event_time": "2024-06-01T01:22:00+00:00"}]], "user_id": ["140083900046208_cd3f6e63-e799-417b-9cbd-2e46e3f54e7f"], "data_processing_context": {"generation_time": "2024-08-14T03:03:56.016539", "watermark": "2024-06-01T01:33:59.999000+00:00"}}
{"visit_id": "140083900046208_16", "start_time": "2024-06-01T00:00:00+00:00", "end_time": "2024-06-01T01:24:00+00:00", "visited_pages": [[{"page": "index", "event_time": "2024-06-01T00:00:00+00:00"}, {"page": "category_4", "event_time": "2024-06-01T00:02:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:03:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:04:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:09:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:14:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:15:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:20:00+00:00"}, {"page": "page_8", "event_time": "2024-06-01T00:24:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:27:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:29:00+00:00"}, {"page": "category_3", "event_time": "2024-06-01T00:30:00+00:00"}, {"page": "category_5", "event_time": "2024-06-01T00:32:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:34:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:39:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:42:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:46:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:48:00+00:00"}, {"page": "category_2", "event_time": "2024-06-01T00:49:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:51:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:54:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:55:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:59:00+00:00"}, {"page": "main", "event_time": "2024-06-01T01:02:00+00:00"}, {"page": "index", "event_time": "2024-06-01T01:07:00+00:00"}, {"page": "category_18", "event_time": "2024-06-01T01:08:00+00:00"}, {"page": "home", "event_time": "2024-06-01T01:11:00+00:00"}, {"page": "home", "event_time": "2024-06-01T01:14:00+00:00"}, {"page": "main", "event_time": "2024-06-01T01:16:00+00:00"}, {"page": "page_1", "event_time": "2024-06-01T01:20:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T01:21:00+00:00"}, {"page": "index", "event_time": "2024-06-01T01:23:00+00:00"}, {"page": "index", "event_time": "2024-06-01T01:24:00+00:00"}]], "user_id": ["140083900046208_712a982f-2c76-4208-848e-ad891e6644b6"], "data_processing_context": {"generation_time": "2024-08-14T03:03:56.022592", "watermark": "2024-06-01T01:33:59.999000+00:00"}}
{"visit_id": "140083900046208_23", "start_time": "2024-06-01T00:00:00+00:00", "end_time": "2024-06-01T01:22:00+00:00", "visited_pages": [[{"page": "contact", "event_time": "2024-06-01T00:00:00+00:00"}, {"page": "category_12", "event_time": "2024-06-01T00:04:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:08:00+00:00"}, {"page": "index", "event_time": "2024-06-01T00:09:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:14:00+00:00"}, {"page": "contact", "event_time": "2024-06-01T00:16:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:19:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:24:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:26:00+00:00"}, {"page": "index", "event_time": "2024-06-01T00:30:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:33:00+00:00"}, {"page": "index", "event_time": "2024-06-01T00:34:00+00:00"}, {"page": "home", "event_time": "2024-06-01T00:36:00+00:00"}, {"page": "category_19", "event_time": "2024-06-01T00:39:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:44:00+00:00"}, {"page": "main", "event_time": "2024-06-01T00:49:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T00:54:00+00:00"}, {"page": "about", "event_time": "2024-06-01T00:55:00+00:00"}, {"page": "category_13", "event_time": "2024-06-01T00:58:00+00:00"}, {"page": "main", "event_time": "2024-06-01T01:03:00+00:00"}, {"page": "category_17", "event_time": "2024-06-01T01:08:00+00:00"}, {"page": "categories", "event_time": "2024-06-01T01:12:00+00:00"}, {"page": "category_12", "event_time": "2024-06-01T01:15:00+00:00"}, {"page": "category_2", "event_time": "2024-06-01T01:18:00+00:00"}, {"page": "main", "event_time": "2024-06-01T01:20:00+00:00"}, {"page": "main", "event_time": "2024-06-01T01:22:00+00:00"}]], "user_id": ["140083900046208_b543abf9-2a3d-4ef0-a2c1-40b2b39bdcea"], "data_processing_context": {"generation_time": "2024-08-14T03:03:56.066462", "watermark": "2024-06-01T01:33:59.999000+00:00"}}
```

⚠️ If some of the windows seem like duplicates, that's normal but they're not duplicates. We're using an `allowed_lateness`
attribute on the session window definition which extends the lifespan of each session window, even if it was already emitted
because of passing behind the watermark. Put differently, each window is kept for 15 more minutes, hoping to integrate late events
after the watermark.