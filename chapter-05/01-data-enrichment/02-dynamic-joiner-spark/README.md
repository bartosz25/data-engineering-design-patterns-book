# Data enrichment - dynamic joiner with Apache Spark Structured Streaming with Apache Kafka

1. Start Apache Kafka broker:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [visits_ads_enrichment_job.py](visits_ads_enrichment_job.py)
* the job combines visits with the ad displays
* the join is dynamic as it involves time boundaries, put different an ad is supposed to be displayed 
  within 2 minutes since the beginning of the visit
* in the demo you'll see that this conditions holds whenever an ad or visit comes first for processing, which
  may happen in real world if one of the producers encounter unexpected latency
3. Start the `visits_ads_enrichment_job.py`
4. Run a console consumer on top of the enriched visits topic:
```
docker exec dedp_ch05_01_kafka kafka-console-consumer.sh --topic visits-enriched --bootstrap-server localhost:9092
```
5. Run console producer for the visits topic:
```
docker exec -ti dedp_ch05_01_kafka kafka-console-producer.sh --topic visits --bootstrap-server localhost:9092
```
6. Run console producer for the ads topic:
```
docker exec -ti dedp_ch05_01_kafka kafka-console-producer.sh --topic ads --bootstrap-server localhost:9092
```
7. Generate the first two visits:
```
{"visit_id": "visit-1", "event_time": "2024-01-01T08:18:00.000+00:00", "page": "home"}
{"visit_id": "visit-2", "event_time": "2024-01-01T08:20:00.000+00:00", "page": "home"}
```
8. Generate the ad for the first visit:
```
{"page": "home", "display_time": "2024-01-01T08:18:15.000+00:00", "campaign_name": "supermarket"}
```

The join should return the matching visit and ad combination as:
```
{"visit_id":"visit-1","event_time":"2024-01-01T09:18:00.000+01:00","visit_page":"home","page":"home","display_time":"2024-01-01T09:18:15.000+01:00","campaign_name":"supermarket"}
```

9. Add new visits to advance the watermark:
```
{"visit_id": "visit-3", "event_time": "2024-01-01T08:58:00.000+00:00", "page": "category1"}
{"visit_id": "visit-4", "event_time": "2024-01-01T09:00:00.000+00:00", "page": "category2"}
```
10. Do the same for the ads:
```
{"page": "category3", "display_time": "2024-01-01T09:00:15.000+00:00", "campaign_name": "supermarket"}
```

As the watermark moved, the job considers there won't be anymore data to match for the expired visits. Since
the join is of the _left outer_ type, the job writes not matched and expired visits to the output topic:
```
{"visit_id":"visit-2","event_time":"2024-01-01T09:20:00.000+01:00","visit_page":"home"}
```
11. Let's simulate now some extra latency on the visits by adding an ad first:
```
{"page": "category4", "display_time": "2024-01-01T09:10:30.000+00:00", "campaign_name": "internet provider"}
```
12. Let's produce now a late visit:
```
{"visit_id": "visit-5", "event_time": "2024-01-01T08:58:00.000+00:00", "page": "category4"}
```
Even though the page condition matches, there is nothing written to the output topic as the timestamp 
condition is not met.
13. Let's fix the time condition join now:
```
{"visit_id": "visit-6", "event_time": "2024-01-01T09:09:00.000+00:00", "page": "category4"}
```
Now the join should happen correctly:
```
{"visit_id":"visit-6","event_time":"2024-01-01T10:09:00.000+01:00","visit_page":"category4","page":"category4","display_time":"2024-01-01T10:10:30.000+01:00","campaign_name":"internet provider"}
```
14. And move the watermark:
```
{"visit_id": "visit-100", "event_time": "2024-01-01T10:09:00.000+00:00", "page": "category6"}
```
The new visit generates buffered and not matched visits 3 and 5:
```
{"visit_id":"visit-5","event_time":"2024-01-01T09:58:00.000+01:00","visit_page":"category4"}
{"visit_id":"visit-3","event_time":"2024-01-01T09:58:00.000+01:00","visit_page":"category1"}
```