# Data enrichment - API with Apache Spark and Apache Kafka

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```
2. Start the API mock by running the `geoloc_api.py`
* the mock returns random countries for a given IP
3. Explain the [visits_enricher_job.py](visits_enricher_job.py)
* the job uses the `KafkaWriterWithEnricher` to enrich the incoming records
  * the enrichment logic relies on a bulk request of 100 IPs at most
    * this optimizes network throughput
  * besides
    * there is a caching logic to avoid querying the same ip multiple times 
    * the logic also deduplicates the ips by wrapping the list to fetch with a set
4. Run the `visits_enricher_job.py`
5. Consume the data from the output topic:
```
docker exec dedp_ch05_bin_packer_kafka kafka-console-consumer.sh --topic visits-enriched --bootstrap-server localhost:9092  

{"raw": {"visit_id": "57572139901176703872_3", "event_time": "2024-01-01T04:36:12+00:00", "user_id": "139901176703872_ad5688e1-07ca-40d3-ac9b-e30ca104b617", "keep_private": false, "page": "contact", "context": {"referral": "Medium", "ad_id": "ad 1", "user": {"ip": "177.184.90.0", "login": "dcampbell", "connected_since": "2023-12-26T00:00:00+00:00"}, "technical": {"browser": "Firefox", "browser_version": "20.2", "network_type": "4G", "device_type": "PC", "device_version": "2.0"}}}}, "country": {"name": "HUNGARY", "code": "HU"}}
{"raw": {"visit_id": "311069139901176703872_4", "event_time": "2024-01-01T03:54:00+00:00", "user_id": "139901176703872_daca7f74-699a-4492-9dd2-333716edb05a", "keep_private": false, "page": "main", "context": {"referral": "Medium", "ad_id": null, "user": {"ip": "166.198.219.194", "login": "valeriehenson"}, "technical": {"browser": "Safari", "browser_version": "25.09", "network_type": "4G", "device_type": "iPhone", "device_version": "5.0"}}}}, "country": {"name": "BARBADOS", "code": "BB"}}
{"raw": {"visit_id": "100981139901176703872_5", "event_time": "2024-01-01T03:51:25+00:00", "user_id": "139901176703872_3f1447d1-236f-40d8-966b-3b563e667364", "keep_private": false, "page": "categories", "context": {"referral": "Facebook", "ad_id": null, "user": {"ip": "81.157.1.123", "login": "amynunez", "connected_since": "2023-12-29T00:00:00+00:00"}, "technical": {"browser": "Safari", "browser_version": "20.0", "network_type": "LAN", "device_type": "MacBook", "device_version": "4.0"}}}}, "country": {"name": "SLOVAKIA", "code": "SK"}}
{"raw": {"visit_id": "249679139901176703872_6", "event_time": "2024-01-01T03:51:25+00:00", "user_id": "139901176703872_75338acf-d663-4cb8-8fe6-6e4d41570b18", "keep_private": false, "page": "home", "context": {"referral": "Google Ads", "ad_id": null, "user": {"ip": "90.47.140.0", "login": "xbarker"}, "technical": {"browser": "Firefox", "browser_version": "23.1", "network_type": "5G", "device_type": "Smartphone", "device_version": "4.0"}}}}, "country": {"name": "PHILIPPINES", "code": "PH"}}
{"raw": {"visit_id": "118810139901176703872_7", "event_time": "2024-01-01T04:10:00+00:00", "user_id": "139901176703872_7eb6b7e3-a1d9-4d8e-a2da-2ae0ae052771", "keep_private": false, "page": "category_11", "context": {"referral": "LinkedIn", "ad_id": "ad 5", "user": {"ip": "205.4.13.129", "login": "ymartin", "connected_since": "2023-12-07T00:00:00+00:00"}, "technical": {"browser": "Safari", "browser_version": "20.0", "network_type": "LAN", "device_type": "iPad", "device_version": "4.0"}}}}, "country": {"name": "FAROE ISLANDS", "code": "FO"}}
{"raw": {"visit_id": "168064139901176703872_8", "event_time": "2024-01-01T04:03:00+00:00", "user_id": "139901176703872_d6898fde-7959-4e96-bcf9-376884d2f228", "keep_private": false, "page": "main", "context": {"referral": "Google Search", "ad_id": null, "user": {"ip": "97.96.252.215", "login": "parkkiara", "connected_since": "2023-12-19T00:00:00+00:00"}, "technical": {"browser": "Chrome", "browser_version": "25.09", "network_type": "LAN", "device_type": "iPad", "device_version": "3.0"}}}}, "country": {"name": "TANZANIA, UNITED REPUBLIC OF", "code": "TZ"}}
{"raw": {"visit_id": "103563139901176703872_9", "event_time": "2024-01-01T04:07:00+00:00", "user_id": "139901176703872_0bd30bbf-5a2f-4db5-b238-6951e6c94dc5", "keep_private": false, "page": "contact", "context": {"referral": "LinkedIn", "ad_id": null, "user": {"ip": "104.248.107.250", "login": "oreeves"}, "technical": {"browser": "Safari", "browser_version": "23.1", "network_type": "5G", "device_type": "Smartphone", "device_version": "3.0"}}}}, "country": {"name": "MADAGASCAR", "code": "MG"}}
```

You should see the country part defined for each visit. As the cache is local to the micro-batch, you might see different countries associated for the same visit.