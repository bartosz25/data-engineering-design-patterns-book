# Data ingestion - API Gateway with Django and an Apache Spark fallback pipeline

1. Explain the [endpoint_data_ingestion_fallback.py](webservice/endpoint_data_ingestion_fallback.py)
* this is the endpoint with some fallback storage mechanism; for the sake of simplicity the code writes each
not delivered batch to a new file; on production you might need to create bigger files by providing some local 
in-memory buffering or by writing a temporary append-only file before flushing it to the final fallback storage
  * both approaches have a drawback though; if before flushing the buffer the API endpoint goes down, you may lose
    the buffered records
2. Explain the [ingestion_pipeline.py](fallback/ingestion_pipeline.py)
* this is our replay pipeline that is going to read all new input files and ingest them to the visits topic
3. Prepare the checkpoint directory:
```
cd docker
rm -rf /tmp/dedp/ch11/01-data-ingestion/01-api-gateway-django
mkdir -p /tmp/dedp/ch11/01-data-ingestion/01-api-gateway-django/fallback/topic=visits
mkdir -p /tmp/dedp/ch11/01-data-ingestion/01-api-gateway-django/checkpoint
```
4. Start Apache Kafka broker:
```
docker-compose down --volumes; docker-compose up
```

5. Start the API Gateway:
```
python api.py
```

> [!WARNING]  
> For the sake of simplicity, the demo doesn't contain any data authorization mechanism. You should definitively avoid this on production!

6. Open Kafka console consumer:
```
docker exec -ti dedp_kafka  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits --from-beginning --property print.headers=true
```

7. Produce some events to the API Gateway:
```
curl -s -X POST -H "Content-Type: application/json" -d '[{"event_id": "click-1", "visit_id": "visit-1", "event_time": "2025-09-04T07:00:00.000000", "page": "index.html"}, {"event_id": "click-2", "visit_id": "visit-1", "event_time": "2025-09-04T07:02:00.000000", "page": "contact.html"}]'  -H "Referer: https://localhost:7070" http://localhost:8080/ingestion/ingest/visits/visit-1
```

In the HTTP response you should see no errors in the delivery:
```
{"success": true, "failed": []}
```

You should also see these new records in the visits topic:
```
source:api,referral:https://localhost:7070      {"event_id": "click-1", "visit_id": "visit-1", "event_time": "2025-09-04T07:00:00.000000", "page": "index.html"}
source:api,referral:https://localhost:7070      {"event_id": "click-2", "visit_id": "visit-1", "event_time": "2025-09-04T07:02:00.000000", "page": "contact.html"}
```

8. Stop the Kafka broker:
```
docker stop dedp_kafka
```

9. Send some other events to the API Gateway:

```
curl -s -X POST -H "Content-Type: application/json" -d '[{"event_id": "click-3", "visit_id": "visit-1", "event_time": "2025-09-04T07:03:00.000000", "page": "message.html"}, {"event_id": "click-4", "visit_id": "visit-1", "event_time": "2025-09-04T07:04:00.000000", "page": "about.html"}]'  -H "Referer: https://localhost:7070" http://localhost:8080/ingestion/ingest/visits/visit-1
```

The HTTP response should be different this time:
```
{"success": false, "failed": [{"key": "visit-1", "payload": {"event_id": "click-3", "visit_id": "visit-1", "event_time": "2025-09-04T07:03:00.000000", "page": "message.html"}}, {"key": "visit-1", "payload": {"event_id": "click-4", "visit_id": "visit-1", "event_time": "2025-09-04T07:04:00.000000", "page": "about.html"}}]}
```

This unsuccessful delivery because of our Apache Kafka unavailability triggers writing the 
not delivered records to files:
```
ls /tmp/dedp/ch11/01-data-ingestion/01-api-gateway-django/fallback/topic=visits
data_1758335043004

cat /tmp/dedp/ch11/01-data-ingestion/01-api-gateway-django/fallback/topic\=visits/data_1758335043004 
{"key": "visit-1", "value": {"event_id": "click-3", "visit_id": "visit-1", "event_time": "2025-09-04T07:03:00.000000", "page": "message.html"}, "referral": "https://localhost:7070"}
{"key": "visit-1", "value": {"event_id": "click-4", "visit_id": "visit-1", "event_time": "2025-09-04T07:04:00.000000", "page": "about.html"}, "referral": "https://localhost:7070"}
```

Again, for the sake of simplicity, we write all not delivered records from a batch in the single file. On production you
could do some local buffering before flushing the content so that you create bigger files for the fallback pipeline.

10. Restart the Kafka broker:
```
docker start dedp_kafka
```
11. Run [ingestion_pipeline.py](fallback/ingestion_pipeline.py). The job should ingest missed records from the fallback pipeline. 
12. Check if new records were added to the consumer. The ones replayed from the fallback ingestion job are 
annotated with _source: replay_:
```
docker exec -ti dedp_kafka  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits --from-beginning --property print.headers=true

source:api,referral:https://localhost:7070      {"event_id": "click-1", "visit_id": "visit-1", "event_time": "2025-09-04T07:00:00.000000", "page": "index.html"}
source:api,referral:https://localhost:7070      {"event_id": "click-2", "visit_id": "visit-1", "event_time": "2025-09-04T07:02:00.000000", "page": "contact.html"}
source:replay,referral:https://localhost:7070   {"event_id": "click-3", "visit_id": "visit-1", "event_time": "2025-09-04T07:03:00.000000", "page": "message.html"}
source:replay,referral:https://localhost:7070   {"event_id": "click-4", "visit_id": "visit-1", "event_time": "2025-09-04T07:04:00.000000", "page": "about.html"}

```






















**Open the project components (fallback-pipeline and web-service) in 2 different PyCharm windows.**
 
1. Start the Docker containers `docker-compose down --volumes; docker-compose up`
2. Go to [http://localhost:803/](http://localhost:803/) and check if it opens correctly. The goal of the exercise
   is to navigate the site and observe the tracking information delivered to our web service.
3. Start the web service from [webservice](web-service%2Fwebservice)
4. Open Kafka console `docker exec -ti docker_kafka_1  kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic cities`
5. Explain the [endpoint_data_ingestion.py](web-service%2Fwebservice%2Fendpoint_data_ingestion.py)
   * It creates a new Kafka producer and an in-memory status holder 
   * By default, each message has the delivery status set to false
   * Whenever we get a success callback, we update the status to true 
   * We remove the list of unsuccessfully delivered messages in the `callback_data_holder.get_failed_deliveries()`
   * When there are some failures, the JavaScript code puts them to the browser local storage for retries
6. Visit at least 5 different pages to produce some data.
7. Change the `INGESTION_ENDPOINT` in the `collect_clicks.js` file. It should point to 'http://localhost:8080/dataingestionfallback/ingest/cities'
8. Navigate the website to see the new endpoint called (e.g. from Developer Tools > Network)
9. Stop the Kafka broker `docker stop docker_kafka_1`
10. From now on, any request reaching the fallback endpoint should write the data to files. 
   * Visit few pages to start the delivery
   * Check the written directory:
   ```
   tree /tmp/bde-snippets-3/lesson3/fallback/
   less /tmp/bde-snippets-3/lesson3/fallback/* 
   ```
11. Restart the Kafka broker `docker start docker_kafka_1`
12. Go to the **fallback-pipeline** project
13. Explain the [ingestion_pipeline.py](fallback-pipeline%2Fingestion_pipeline.py)
   * `trigger(availableNow=True)` with the `checkpointLocation` to ensure not processing the same files again
   * `repartition` with the `sortWithinPartitions` to deliver data in order for each key
14. Restart the cities console consumer `docker exec -ti docker_kafka_1  kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic cities`
15. Start the job and switch to the console. You should see data from the fallback storage coming in.
