# Dead-Letter - streaming job

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```
At this moment, you should have a running data generator and three Kafka topics: visits, visits_reduced, visits_dlq.
2. Explain the [dead_letter_job.py](dead_letter_job.py)
* the job does a simple mapping by extracting few fields from the input visit message
* however, as things may go wrong due to non-existent fields referenced in the `map` function
  * to handle these non-transient errors, the job relies on a _side output_; it enables writing one processed record
    into multiple different data stores
3. Start the `dead_letter_job.py`
4. Start two Kafka consumers:
* for the reduced, hence valid, visits:
  `docker exec -ti dedp_ch03_dlq_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits_reduced`
* for the unprocessable visits:
  `docker exec -ti dedp_ch03_dlq_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits_dlq`
Both consumers should get some data. You can find the examples below:
```
# visits_reduced

{"visit_id": "140380747557760_40", "event_time": 1698819673, "page": "contact"}
{"visit_id": "140380747557760_41", "event_time": 1698819794, "page": "about"}
{"visit_id": "140380747557760_42", "event_time": 1698819660, "page": "category_15"}
{"visit_id": "140380747557760_43", "event_time": 1698817920, "page": "index"}
{"visit_id": "140380747557760_48", "event_time": 1698818640, "page": "contact"}


# visits_dlq
{"processing_time": "2024-01-13T04:21:50.037894", "input_json": "{\"visit_id\": null, \"event_time\": null, \"user_id\": null, \"keep_private\": null, \"page\": \"page_8\", \"context\": null}", "error_message": "fromisoformat: argument must be str"}
{"processing_time": "2024-01-13T04:21:50.038122", "input_json": "{\"visit_id\": \"140380747557760_50\", \"event_time\": null, \"user_id\": null, \"keep_private\": null, \"page\": null, \"context\": null}", "error_message": "fromisoformat: argument must be str"}
{"processing_time": "2024-01-13T04:21:50.040689", "input_json": "{\"visit_id\": null, \"event_time\": null, \"user_id\": null, \"keep_private\": null, \"page\": \"contact\", \"context\": null}", "error_message": "fromisoformat: argument must be str"}
{"processing_time": "2024-01-13T04:21:50.041041", "input_json": "{\"visit_id\": null, \"event_time\": null, \"user_id\": null, \"keep_private\": null, \"page\": null, \"context\": null}", "error_message": "fromisoformat: argument must be str"}
```