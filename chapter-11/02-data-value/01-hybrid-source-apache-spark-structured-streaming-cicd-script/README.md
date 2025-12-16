# Hybrid source - Apache Spark and a CI/CD script

1. Prepare the historical dataset:
```
rm -rf /tmp/dedp/ch11/11-streaming/02-hybrid-source-apache-spark-structured-streaming-cicd-script/
cd dataset
mkdir -p /tmp/dedp/ch11/11-streaming/02-hybrid-source-apache-spark-structured-streaming-cicd-script/input
cd historical/
docker-compose down --volumes; docker-compose up
```

2. Explain [run_hybrid_source_data_loading.py](run_hybrid_source_data_loading.py)
* the job simulates the Hybrid source design pattern with a deployment script
* it first reads the historical data source which are JSON files, and loads them to the Delta Lake table
* since the dataset is bounded, the first job stops and the script starts the real-time job; here for the sake of simplicity,
we're using the simple Python script that will be running as long as the real-time script. On your real environment you will
deploy those jobs on a cluster so that you can terminate the script once the real-time job is up and running

3. Start the real-time data dataset:
```
cd ../real-time
docker-compose down --volumes; docker-compose up
```

4. Run the `run_hybrid_source_data_loading.py`

5. Stop the `run_hybrid_source_data_loading.py` after a few minutes and run the `delta_table_reader.py`. You should see
data coming from both historical and real-time data sources in the table:
```
+-----------------+-------------------+----------------------------------------------------+-----------+---------------------------------------------------------------------------------------------------------+
|visit_id         |event_time         |user_id                                             |page       |context                                                                                                  |
+-----------------+-------------------+----------------------------------------------------+-----------+---------------------------------------------------------------------------------------------------------+
|135357500644224_0|2024-09-01 02:00:00|135357500644224_f7ace007-f616-4a25-b13e-b89822a9fa5a|home       |{Google Ads, ad 4, {87.45.222.0, pughjonathon, 2024-08-29 02:00:00}, {Safari, 20.0, Wi-Fi, iPhone, 1.0}} |
|135357500644224_1|2024-09-01 02:00:00|135357500644224_33650873-8f47-48ac-8159-76d95af18ad8|about      |{Medium, ad 2, {45.224.0.77, toddharrison, NULL}, {Chrome, 21.0, 4G, PC, 6.0}}                           |
|135357500644224_2|2024-09-01 02:00:00|135357500644224_1397dd61-9a1d-4b0a-b8e5-e896973869f5|about      |{Google Search, ad 2, {165.164.150.242, gmitchell, NULL}, {Firefox, 23.0, Wi-Fi, iPad, 2.0}}             |
|135357500644224_3|2024-09-01 02:00:00|135357500644224_089397f9-3ad1-4fa2-ac55-18c076d5bc28|contact    |{Medium, ad 3, {166.210.251.84, ricardo99, 2024-08-08 02:00:00}, {Firefox, 25.09, LAN, MacBook, 4.0}}    |
|135357500644224_4|2024-09-01 02:00:00|135357500644224_06da69e4-192f-410f-a552-eadc266ed1fa|main       |{LinkedIn, ad 4, {56.197.127.239, daniellarson, NULL}, {Chrome, 22.0, 4G, MacBook, 6.0}}                 |
|135357500644224_5|2024-09-01 02:00:00|135357500644224_17b88ede-d81d-488d-b571-124f05f4b2cd|category_5 |{YouTube, ad 2, {65.153.18.168, alejandrovaughan, NULL}, {Safari, 25.09, Wi-Fi, Smartphone, 2.0}}        |
|135357500644224_6|2024-09-01 02:00:00|135357500644224_cca778fb-1f88-4639-bf78-940fc2ce3e63|home       |{Medium, ad 4, {114.239.139.197, susan82, 2024-08-07 02:00:00}, {Firefox, 20.2, Wi-Fi, MacBook, 4.0}}    |
|135357500644224_7|2024-09-01 02:00:00|135357500644224_82e56df6-c828-4aed-91d8-de4818cb1db3|index      |{LinkedIn, NULL, {175.150.38.98, berrywayne, NULL}, {Safari, 24.11, LAN, MacBook, 4.0}}                  |
|135357500644224_8|2024-09-01 02:00:00|135357500644224_91bca04b-bcbe-407e-9dab-e0d82132f34a|category_14|{Facebook, ad 5, {201.63.210.215, qvazquez, 2024-08-19 02:00:00}, {Safari, 23.2, Wi-Fi, iPhone, 6.0}}    |
|135357500644224_9|2024-09-01 02:00:00|135357500644224_0edc1834-b57b-407b-82a7-53ebb6b54d90|contact    |{Google Search, ad 3, {64.22.51.206, bedwards, 2024-08-20 02:00:00}, {Chrome, 24.11, 5G, iPad, 5.0}}     |
|136982235200384_1|2025-09-01 03:03:00|136982235200384_a36efdb3-a23f-4f85-afb8-2af1537da13b|about      |{NULL, ad 4, {102.105.193.232, danielle33, NULL}, {Firefox, 21.0, 4G, iPad, 1.0}}                        |
|136982235200384_1|2025-09-01 03:04:00|136982235200384_a36efdb3-a23f-4f85-afb8-2af1537da13b|index      |{NULL, ad 4, {102.105.193.232, danielle33, NULL}, {Firefox, 21.0, 4G, iPad, 1.0}}                        |
|136982235200384_1|2025-09-01 03:09:00|136982235200384_a36efdb3-a23f-4f85-afb8-2af1537da13b|main       |{NULL, ad 4, {102.105.193.232, danielle33, NULL}, {Firefox, 21.0, 4G, iPad, 1.0}}                        |
|136982235200384_0|2025-09-01 03:17:00|136982235200384_2a1ee0aa-ba46-482c-b162-4260f1d40454|page_18    |{Twitter, NULL, {195.106.62.189, thomas36, 2025-08-29 02:00:00}, {Firefox, 23.2, Wi-Fi, Smartphone, 4.0}}|
|136982235200384_0|2025-09-01 03:18:00|136982235200384_2a1ee0aa-ba46-482c-b162-4260f1d40454|page_14    |{Twitter, NULL, {195.106.62.189, thomas36, 2025-08-29 02:00:00}, {Firefox, 23.2, Wi-Fi, Smartphone, 4.0}}|
|136982235200384_0|2025-09-01 03:20:00|136982235200384_2a1ee0aa-ba46-482c-b162-4260f1d40454|home       |{Twitter, NULL, {195.106.62.189, thomas36, 2025-08-29 02:00:00}, {Firefox, 23.2, Wi-Fi, Smartphone, 4.0}}|
+-----------------+-------------------+----------------------------------------------------+-----------+---------------------------------------------------------------------------------------------------------+
```