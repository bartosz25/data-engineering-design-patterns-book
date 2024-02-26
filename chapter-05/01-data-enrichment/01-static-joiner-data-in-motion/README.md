# Data enrichment - joining static data with data in-motion

1. Start Apache Kafka broker and generate the dataset:
```
mkdir -p /tmp/dedp/ch05/01-data-enrichment/01-static-joiner-data-in-motion/input
cd docker
docker-compose down --volumes; docker-compose up
```
2. Create the devices Delta Lake table by running the `devices_table_creator.py`
* we rely on a Delta table here because it provides ACID guarantees, i.e. it doesn't require the table writing 
process to be synchronized with the streaming job
  * if we used a JSON file format here, the streaming job would require a synchronization, i.e. whenever new files are
    written, we should stop the job and resume once the writing process succeeds; otherwise, we would risk having
    partially visible dataset
3. Explain the [visits_enricher.py](visits_enricher.py)
4. Run the `visits_enricher.py`
5. Check the results:
```
docker exec -ti dedp_ch05_01_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits_enriched

{"visit":"{\"visit_id\": \"139702650116992_5\", \"event_time\": \"2023-11-24T07:35:00+00:00\", \"user_id\": \"139702650116992_30afa33e-7996-437e-ba04-54bfc85cfc3b\", \"keep_private\": false, \"page\": \"contact\", \"context\": {\"referral\": \"Twitter\", \"ad_id\": \"ad 2\", \"user\": {\"ip\": \"112.224.35.249\", \"login\": \"lyonsadrian\", \"connected_since\": null}, \"technical\": {\"browser\": \"Chrome\", \"browser_version\": \"24.11\", \"network_type\": \"Wi-Fi\", \"device_type\": \"galaxy\", \"device_version\": \"Android 12L\"}}}","device":{"version":"Android 12L","type":"galaxy","full_name":"Galaxy S 4g"}}
{"visit":"{\"visit_id\": \"139702650116992_4\", \"event_time\": \"2023-11-24T08:42:06+00:00\", \"user_id\": \"139702650116992_67602a8f-d131-45fc-b836-83635eaa4711\", \"keep_private\": false, \"page\": \"home\", \"context\": {\"referral\": \"Google Search\", \"ad_id\": \"ad 1\", \"user\": {\"ip\": \"78.102.48.14\", \"login\": \"jennifer63\", \"connected_since\": null}, \"technical\": {\"browser\": \"Safari\", \"browser_version\": \"24.11\", \"network_type\": \"Wi-Fi\", \"device_type\": \"htc\", \"device_version\": \"Android 12\"}}}","device":{"version":"Android 12","type":"htc","full_name":"Sensation 4g"}}
{"visit":"{\"visit_id\": \"139702650116992_5\", \"event_time\": \"2023-11-24T07:38:00+00:00\", \"user_id\": \"139702650116992_30afa33e-7996-437e-ba04-54bfc85cfc3b\", \"keep_private\": false, \"page\": \"page_14\", \"context\": {\"referral\": \"Twitter\", \"ad_id\": \"ad 2\", \"user\": {\"ip\": \"112.224.35.249\", \"login\": \"lyonsadrian\", \"connected_since\": null}, \"technical\": {\"browser\": \"Chrome\", \"browser_version\": \"24.11\", \"network_type\": \"Wi-Fi\", \"device_type\": \"galaxy\", \"device_version\": \"Android 12L\"}}}","device":{"version":"Android 12L","type":"galaxy","full_name":"Galaxy S 4g"}}
{"visit":"{\"visit_id\": \"139702650116992_4\", \"event_time\": \"2023-11-24T08:44:06+00:00\", \"user_id\": \"139702650116992_67602a8f-d131-45fc-b836-83635eaa4711\", \"keep_private\": false, \"page\": \"home\", \"context\": {\"referral\": \"Google Search\", \"ad_id\": \"ad 1\", \"user\": {\"ip\": \"78.102.48.14\", \"login\": \"jennifer63\", \"connected_since\": null}, \"technical\": {\"browser\": \"Safari\", \"browser_version\": \"24.11\", \"network_type\": \"Wi-Fi\", \"device_type\": \"htc\", \"device_version\": \"Android 12\"}}}","device":{"version":"Android 12","type":"htc","full_name":"Sensation 4g"}}
{"visit":"{\"visit_id\": \"139702650116992_0\", \"event_time\": \"2023-11-24T08:54:20+00:00\", \"user_id\": \"139702650116992_ef93ff73-8073-4bcd-9b1e-927aea77d295\", \"keep_private\": false, \"page\": \"about\", \"context\": {\"referral\": \"Twitter\", \"ad_id\": null, \"user\": {\"ip\": \"93.151.173.182\", \"login\": \"cindy25\", \"connected_since\": \"2023-11-03T00:00:00+00:00\"}, \"technical\": {\"browser\": \"Safari\", \"browser_version\": \"25.09\", \"network_type\": \"LAN\", \"device_type\": \"galaxy\", \"device_version\": \"Android 12L\"}}}","device":{"version":"Android 12L","type":"galaxy","full_name":"Galaxy S 4g"}}
{"visit":"{\"visit_id\": \"139702650116992_1\", \"event_time\": \"2023-11-24T08:29:00+00:00\", \"user_id\": \"139702650116992_bf366de3-515d-4f79-96ff-b36dc477e099\", \"keep_private\": false, \"page\": \"contact\", \"context\": {\"referral\": \"Google Ads\", \"ad_id\": null, \"user\": {\"ip\": \"2.225.221.212\", \"login\": \"vroberts\", \"connected_since\": null}, \"technical\": {\"browser\": \"Firefox\", \"browser_version\": \"25.09\", \"network_type\": \"Wi-Fi\", \"device_type\": \"galaxy\", \"device_version\": \"Android 12\"}}}","device":{"version":"Android 12","type":"galaxy","full_name":"Galaxy S 4g"}}
```