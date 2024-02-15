# Data decoration - Spark wrapper

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```
2. Explain the [wrapper_decorator_job.py](wrapper_decorator_job.py)
* the job uses the `foreachBatch` sink to decorate the incoming rows; for the sake of simplicity, it doesn't
  process anything
  * the raw data will be written as string as we don't want to manipulate it to avoid losing the original attributes
3. Start the `wrapper_decorator_job.py`
4. Run a console consumer on top of the decorated visits topic:
```
docker exec dedp_ch05_decorator_kafka kafka-console-consumer.sh --topic visits-decorated --bootstrap-server localhost:9092 --property print.headers=true
```

You should see the records with metadata attributes added to the structure: 
```
{"raw_data":"{\"visit_id\": \"139785490205568_9\", \"event_time\": \"2024-01-01T00:57:00+00:00\", \"user_id\": \"139785490205568_08fdd6d8-9b79-4ce5-87e2-e04232f453be\", \"keep_private\": false, \"page\": \"category_4\", \"context\": {\"referral\": \"LinkedIn\", \"ad_id\": \"ad 3\", \"user\": {\"ip\": \"149.128.93.118\", \"login\": \"rsalas\", \"connected_since\": null}, \"technical\": {\"browser\": \"Safari\", \"browser_version\": \"25.09\", \"network_type\": \"5G\", \"device_type\": \"MacBook\", \"device_version\": \"6.0\"}}}","processing_context":{"job_version":"v1.0.3","batch_version":2}}
{"raw_data":"{\"visit_id\": \"139785490205568_4\", \"event_time\": \"2024-01-01T00:53:01+00:00\", \"user_id\": \"139785490205568_abeeb2f3-8a2b-46ff-b44b-7a4deff0873f\", \"keep_private\": false, \"page\": \"category_2\", \"context\": {\"referral\": \"Google Ads\", \"ad_id\": null, \"user\": {\"ip\": \"58.233.175.241\", \"login\": \"lhall\", \"connected_since\": null}, \"technical\": {\"browser\": \"Safari\", \"browser_version\": \"20.0\", \"network_type\": \"4G\", \"device_type\": \"PC\", \"device_version\": \"2.0\"}}}","processing_context":{"job_version":"v1.0.3","batch_version":3}}
{"raw_data":"{\"visit_id\": \"139785490205568_0\", \"event_time\": \"2024-01-01T01:01:00+00:00\", \"user_id\": \"139785490205568_017de55f-36ff-4ebf-a5d4-783157e1ae1b\", \"keep_private\": false, \"page\": \"categories\", \"context\": {\"referral\": \"Facebook\", \"ad_id\": null, \"user\": {\"ip\": \"23.41.227.227\", \"login\": \"james16\", \"connected_since\": null}, \"technical\": {\"browser\": \"Safari\", \"browser_version\": \"23.0\", \"network_type\": \"LAN\", \"device_type\": \"MacBook\", \"device_version\": \"6.0\"}}}","processing_context":{"job_version":"v1.0.3","batch_version":3}}
{"raw_data":"{\"visit_id\": \"139785490205568_6\", \"event_time\": \"2024-01-01T01:05:00+00:00\", \"user_id\": \"139785490205568_2af3e48f-0100-4222-baaf-e09b22125862\", \"keep_private\": false, \"page\": \"categories\", \"context\": {\"referral\": \"StackOverflow\", \"ad_id\": \"ad 2\", \"user\": {\"ip\": \"91.13.232.166\", \"login\": \"amanda28\", \"connected_since\": \"2023-12-08T00:00:00+00:00\"}, \"technical\": {\"browser\": \"Safari\", \"browser_version\": \"23.1\", \"network_type\": \"4G\", \"device_type\": \"PC\", \"device_version\": \"5.0\"}}}","processing_context":{"job_version":"v1.0.3","batch_version":3}}
{"raw_data":"{\"visit_id\": \"139785490205568_3\", \"event_time\": \"2024-01-01T01:08:00+00:00\", \"user_id\": \"139785490205568_d31a8c16-1e19-4017-b6b9-39205db57500\", \"keep_private\": false, \"page\": \"category_13\", \"context\": {\"referral\": \"Twitter\", \"ad_id\": \"ad 3\", \"user\": {\"ip\": \"122.114.157.154\", \"login\": \"suzannebailey\", \"connected_since\": null}, \"technical\": {\"browser\": \"Chrome\", \"browser_version\": \"20.1\", \"network_type\": \"LAN\", \"device_type\": \"Smartphone\", \"device_version\": \"5.0\"}}}","processing_context":{"job_version":"v1.0.3","batch_version":3}}
{"raw_data":"{\"visit_id\": \"139785490205568_7\", \"event_time\": \"2024-01-01T01:01:00+00:00\", \"user_id\": \"139785490205568_8984c8c6-3181-45fa-9baf-5f06feb9a76a\", \"keep_private\": false, \"page\": \"home\", \"context\": {\"referral\": \"Google Search\", \"ad_id\": null, \"user\": {\"ip\": \"128.97.247.214\", \"login\": \"bhouston\", \"connected_since\": \"2023-12-27T00:00:00+00:00\"}, \"technical\": {\"browser\": \"Firefox\", \"browser_version\": \"25.09\", \"network_type\": \"LAN\", \"device_type\": \"iPhone\", \"device_version\": \"3.0\"}}}","processing_context":{"job_version":"v1.0.3","batch_version":3}}
```
