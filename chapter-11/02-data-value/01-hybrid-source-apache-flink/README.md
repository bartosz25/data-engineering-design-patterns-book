# Hybrid Source - Apache Flink

1. Prepare the historical dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch11/11-streaming/01-hybrid-source-apache-flink/input
cd historical/
docker-compose down --volumes; docker-compose up
```

2. Explain the [hybrid_source_job.py](hybrid_source_job.py)
* the job defines a `switch_timestamp` attribute to represent when Apache Kafka, hence called real-time reader,
will start; in our example we have to process the full historical dataset and start the real-time reader from the beginning
so the `switch_timestamp` is set to the earliest offset. In case this offset may overlap with the data included in the
file, you will have to implement some filtering logic as the step condition in the historical reader, or simply select
only a bunch of files with the relevant data to process. Additionally, the real-time reader will need to have a timestamp
or offset set so that it can take the records not overlapping with the historical data.
* to combine both data sources in one, the job uses a `HybridSource` where the historical data source is defined 
first

3. Start the real-time data dataset:
```
cd ../real-time
docker-compose down --volumes; docker-compose up
```

4. Start `hybrid_source_job.py`. The job should print the data coming from both data sources, in order, 
starting with the historical data source first:
```
# Historical data starts on 2024-09-01...
1> {"visit_id": "129369615084416_0", "event_time": "2024-09-01T00:00:00+00:00", "user_id": "129369615084416_d2808c83-3914-439c-bb32-bcc126c12cf2", "keep_private": false, "page": "category_19", "context": {"referral": "Medium", "ad_id": "ad 5", "user": {"ip": "171.97.165.104", "login": "williamsexton", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "22.0", "network_type": "LAN", "device_type": "MacBook", "device_version": "4.0"}}} 
1> {"visit_id": "129369615084416_1", "event_time": "2024-09-01T00:00:00+00:00", "user_id": "129369615084416_18c33539-db9e-4048-985c-c715bde8a710", "keep_private": false, "page": "categories", "context": {"referral": "Medium", "ad_id": null, "user": {"ip": "138.9.73.158", "login": "kathleenrichardson", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "24.11", "network_type": "Wi-Fi", "device_type": "MacBook", "device_version": "5.0"}}} 
1> {"visit_id": "129369615084416_2", "event_time": "2024-09-01T00:00:00+00:00", "user_id": "129369615084416_a45f5af6-e278-496f-89b2-cc2f4aed7a03", "keep_private": false, "page": "page_5", "context": {"referral": "StackOverflow", "ad_id": null, "user": {"ip": "26.166.141.110", "login": "crystalsutton", "connected_since": "2024-08-10T00:00:00+00:00"}, "technical": {"browser": "Safari", "browser_version": "23.1", "network_type": "4G", "device_type": "iPad", "device_version": "6.0"}}} 
1> {"visit_id": "129369615084416_3", "event_time": "2024-09-01T00:00:00+00:00", "user_id": "129369615084416_d668c00c-a60e-4a76-a5bd-e4ae9399f606", "keep_private": false, "page": "contact", "context": {"referral": "Google Ads", "ad_id": null, "user": {"ip": "171.232.108.95", "login": "meganpeters", "connected_since": "2024-08-09T00:00:00+00:00"}, "technical": {"browser": "Chrome", "browser_version": "20.2", "network_type": "4G", "device_type": "Smartphone", "device_version": "4.0"}}} 
1> {"visit_id": "129369615084416_4", "event_time": "2024-09-01T00:00:00+00:00", "user_id": "129369615084416_ab388aa1-7e9c-497d-bacb-362de2370c39", "keep_private": true, "page": "index", "context": {"referral": "Google Ads", "ad_id": "ad 4", "user": {"ip": "52.12.22.47", "login": "debbiehubbard", "connected_since": "2024-08-18T00:00:00+00:00"}, "technical": {"browser": "Firefox", "browser_version": "24.11", "network_type": "Wi-Fi", "device_type": "iPad", "device_version": "3.0"}}} 
1> {"visit_id": "129369615084416_5", "event_time": "2024-09-01T00:00:00+00:00", "user_id": "129369615084416_21408a19-6642-4bee-8bf3-62b53901db40", "keep_private": false, "page": "index", "context": {"referral": "LinkedIn", "ad_id": "ad 1", "user": {"ip": "190.241.247.168", "login": "mary20", "connected_since": "2024-08-07T00:00:00+00:00"}, "technical": {"browser": "Firefox", "browser_version": "21.0", "network_type": "Wi-Fi", "device_type": "iPhone", "device_version": "3.0"}}} 
1> {"visit_id": "129369615084416_6", "event_time": "2024-09-01T00:00:00+00:00", "user_id": "129369615084416_451a6743-2bb0-479a-bd33-78b642a4f74e", "keep_private": false, "page": "page_16", "context": {"referral": null, "ad_id": null, "user": {"ip": "159.4.187.173", "login": "browningmark", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "23.1", "network_type": "LAN", "device_type": "MacBook", "device_version": "3.0"}}} 
1> {"visit_id": "129369615084416_7", "event_time": "2024-09-01T00:00:00+00:00", "user_id": "129369615084416_ccfad4c8-254c-447a-90e4-3d1cc4a8789c", "keep_private": false, "page": "main", "context": {"referral": "Facebook", "ad_id": "ad 2", "user": {"ip": "140.204.42.75", "login": "montgomerylee", "connected_since": null}, "technical": {"browser": "Safari", "browser_version": "20.2", "network_type": "LAN", "device_type": "iPhone", "device_version": "1.0"}}} 
1> {"visit_id": "129369615084416_8", "event_time": "2024-09-01T00:00:00+00:00", "user_id": "129369615084416_9b0612db-7862-4ff8-9018-b61abe6bb235", "keep_private": false, "page": "category_5", "context": {"referral": "Google Search", "ad_id": null, "user": {"ip": "130.219.7.101", "login": "cynthialane", "connected_since": null}, "technical": {"browser": "Firefox", "browser_version": "23.2", "network_type": "Wi-Fi", "device_type": "PC", "device_version": "3.0"}}} 
1> {"visit_id": "129369615084416_9", "event_time": "2024-09-01T00:00:00+00:00", "user_id": "129369615084416_76436520-be8f-41e9-b282-aee7fdaa3293", "keep_private": false, "page": "category_17", "context": {"referral": "Google Ads", "ad_id": null, "user": {"ip": "185.77.170.120", "login": "morgancharles", "connected_since": "2024-08-23T00:00:00+00:00"}, "technical": {"browser": "Chrome", "browser_version": "23.0", "network_type": "5G", "device_type": "iPad", "device_version": "5.0"}}}
#...and the real-time data starts one year later
1> {"visit_id": "139248799058816_0", "event_time": "2025-09-01T00:00:00+00:00", "user_id": "139248799058816_ed437e6b-6534-4e6d-a8d0-ab93f5a4a466", "keep_private": false, "page": "about", "context": {"referral": "Google Search", "ad_id": "ad 2", "user": {"ip": "13.116.144.228", "login": "carloscervantes", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "23.0", "network_type": "LAN", "device_type": "MacBook", "device_version": "6.0"}}} 
1> {"visit_id": "139248799058816_1", "event_time": "2025-09-01T00:00:00+00:00", "user_id": "139248799058816_4b8f8144-181c-4db4-a145-116bece2ca20", "keep_private": false, "page": "contact", "context": {"referral": "LinkedIn", "ad_id": "ad 3", "user": {"ip": "25.27.175.100", "login": "kflores", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "23.0", "network_type": "LAN", "device_type": "MacBook", "device_version": "1.0"}}} 
1> {"visit_id": "139248799058816_0", "event_time": "2025-09-01T00:01:00+00:00", "user_id": "139248799058816_ed437e6b-6534-4e6d-a8d0-ab93f5a4a466", "keep_private": false, "page": "contact", "context": {"referral": "Google Search", "ad_id": "ad 2", "user": {"ip": "13.116.144.228", "login": "carloscervantes", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "23.0", "network_type": "LAN", "device_type": "MacBook", "device_version": "6.0"}}} 
1> {"visit_id": "139248799058816_1", "event_time": "2025-09-01T00:03:00+00:00", "user_id": "139248799058816_4b8f8144-181c-4db4-a145-116bece2ca20", "keep_private": false, "page": "about", "context": {"referral": "LinkedIn", "ad_id": "ad 3", "user": {"ip": "25.27.175.100", "login": "kflores", "connected_since": null}, "technical": {"browser": "Chrome", "browser_version": "23.0", "network_type": "LAN", "device_type": "MacBook", "device_version": "1.0"}}}
```