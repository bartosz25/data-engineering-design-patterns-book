# Data removal - in-place overwriter with Delta Lake

1. Generate the test data:
```
cd dataset
mkdir -p /tmp/dedp/ch07/01-personal-data-removal/02-in-place-overwrite-delta-lake/input
docker-compose down --volumes; docker-compose up
```
2. Explain the [visits_delta_table_creator.py](visits_delta_table_creator.py)
* this simple job creates a new Delta Lake table for the visits dataset
3. Run the `visits_delta_table_creator.py`
4. Run the `visits_table_reader.py` to check if the table was created.
```
+------------------+-------------------+----------------------------------------------------+----------+-----------------------------------------------------------------------------------------------------------+
|visit_id          |event_time         |user_id                                             |page      |context                                                                                                    |
+------------------+-------------------+----------------------------------------------------+----------+-----------------------------------------------------------------------------------------------------------+
|139621130423168_39|2023-11-01 01:00:00|139621130423168_029fba78-15dc-4944-9f65-00636566f75b|main      |{YouTube, ad 5, {113.214.255.180, lisabrown, NULL}, {Firefox, 23.2, LAN, MacBook, 1.0}}                    |
|139621130423168_23|2023-11-01 01:00:00|139621130423168_09879afe-9d17-435f-8317-28c46ab60888|contact   |{YouTube, ad 4, {88.88.203.156, tammygalloway, NULL}, {Safari, 24.11, LAN, PC, 1.0}}                       |
|139621130423168_34|2023-11-01 01:00:00|139621130423168_26a6620b-b532-4ba0-80d8-f47678185335|category_2|{Medium, ad 5, {223.240.231.131, antonioarmstrong, 2023-10-25 02:00:00}, {Chrome, 24.11, 5G, MacBook, 2.0}}|
|139621130423168_28|2023-11-01 01:00:00|139621130423168_2acd5594-725d-464a-b71a-743cfbd55fbf|page_19   |{NULL, NULL, {2.93.114.134, nicolethompson, NULL}, {Safari, 23.0, Wi-Fi, Smartphone, 3.0}}                 |
|139621130423168_21|2023-11-01 01:00:00|139621130423168_3414ab9d-30ba-4201-85ba-3c4857dcb00d|about     |{YouTube, ad 4, {166.220.148.86, hughesjody, 2023-10-20 02:00:00}, {Safari, 20.0, 5G, MacBook, 6.0}}       |
|139621130423168_38|2023-11-01 01:00:00|139621130423168_36b0be24-1b1f-48ca-bedb-5ace952b713e|category_5|{Google Ads, ad 1, {45.82.19.34, brettlynch, NULL}, {Firefox, 25.09, 5G, Smartphone, 1.0}}                 |
|139621130423168_25|2023-11-01 01:00:00|139621130423168_3b42f2b8-bca4-423e-bb37-f50874f47dbe|home      |{Google Search, ad 5, {180.88.11.239, xbrown, NULL}, {Chrome, 23.1, 4G, iPad, 5.0}}                        |
|139621130423168_20|2023-11-01 01:00:00|139621130423168_40f71080-70dd-4684-a4ba-f3cf29c7ce4d|main      |{StackOverflow, ad 1, {193.147.168.198, landryjames, NULL}, {Chrome, 21.0, Wi-Fi, PC, 5.0}}                |
|139621130423168_48|2023-11-01 01:00:00|139621130423168_485dc82c-939b-4ea3-a1bb-3e77b59e289f|page_15   |{Google Ads, NULL, {75.124.60.112, jamesrobertson, NULL}, {Safari, 20.1, LAN, iPad, 4.0}}                  |
|139621130423168_45|2023-11-01 01:00:00|139621130423168_48e3facb-ba04-4138-abb4-62dfa00107a5|contact   |{YouTube, NULL, {130.184.106.6, samantha74, NULL}, {Firefox, 23.0, Wi-Fi, Smartphone, 5.0}}                |
|139621130423168_33|2023-11-01 01:00:00|139621130423168_4ecfda49-ac01-4f18-88c8-69c18d6ecfc1|home      |{Twitter, ad 4, {131.239.231.211, zevans, NULL}, {Safari, 20.0, Wi-Fi, PC, 3.0}}                           |
|139621130423168_5 |2023-11-01 01:00:00|139621130423168_517a5668-7572-419f-a59b-bd437b267d84|home      |{YouTube, ad 1, {56.71.225.12, joe32, NULL}, {Firefox, 23.1, 4G, PC, 5.0}}                                 |
|139621130423168_44|2023-11-01 01:00:00|139621130423168_547df5cd-2e49-4a6b-90e0-4aff2535e10e|categories|{YouTube, ad 1, {149.215.206.238, ann22, 2023-10-20 02:00:00}, {Chrome, 20.0, LAN, PC, 4.0}}               |
|139621130423168_14|2023-11-01 01:00:00|139621130423168_54c8d612-1202-48f8-b690-91bccd119941|home      |{Google Ads, ad 1, {75.161.252.118, warrenjacqueline, 2023-10-03 02:00:00}, {Firefox, 25.09, 5G, PC, 2.0}} |
|139621130423168_19|2023-11-01 01:00:00|139621130423168_5a159158-f91d-4c6d-9df9-0855717d8bfa|category_2|{NULL, ad 2, {64.84.83.120, shariberry, NULL}, {Safari, 22.0, Wi-Fi, MacBook, 3.0}}                        |
|139621130423168_32|2023-11-01 01:00:00|139621130423168_5eaea3bb-ef5c-4b8d-bf61-c374a38826bb|page_1    |{LinkedIn, NULL, {183.105.213.80, bridgeseric, NULL}, {Chrome, 20.0, Wi-Fi, Smartphone, 6.0}}              |
|139621130423168_29|2023-11-01 01:00:00|139621130423168_63bec493-88d4-4f24-88f0-1bf1dd572121|categories|{Google Ads, NULL, {189.37.14.158, stacey81, NULL}, {Safari, 20.1, 4G, iPad, 2.0}}                         |
|139621130423168_30|2023-11-01 01:00:00|139621130423168_6489958a-46f5-44a9-8a01-24433f03a040|categories|{YouTube, ad 2, {136.31.174.157, jeanette20, 2023-10-06 02:00:00}, {Firefox, 23.2, Wi-Fi, PC, 1.0}}        |
|139621130423168_37|2023-11-01 01:00:00|139621130423168_67b20d55-ebfc-475c-ac76-b38d450f11de|index     |{StackOverflow, NULL, {67.129.64.8, susanevans, NULL}, {Safari, 20.1, LAN, PC, 3.0}}                       |
|139621130423168_18|2023-11-01 01:00:00|139621130423168_6926f263-2443-49a3-9947-116b0989b281|main      |{Facebook, NULL, {132.101.81.179, ecannon, 2023-10-02 02:00:00}, {Chrome, 23.1, 5G, iPad, 5.0}}            |
+------------------+-------------------+----------------------------------------------------+----------+-----------------------------------------------------------------------------------------------------------+
only showing top 20 rows
```

5. Explain the [user_ip_cleaner.py](user_cleaner.py)
* the job removes the visit for the identified user
6. Identify a user id (e.g. 139621130423168_029fba78-15dc-4944-9f65-00636566f75b) and run the `user_ip_cleaner.py`
7. Run the `visits_table_reader.py` to check if the user data was removed:
```
# 139621130423168_029fba78-15dc-4944-9f65-00636566f75b should be missing

+------------------+-------------------+----------------------------------------------------+----------+-----------------------------------------------------------------------------------------------------------+
|visit_id          |event_time         |user_id                                             |page      |context                                                                                                    |
+------------------+-------------------+----------------------------------------------------+----------+-----------------------------------------------------------------------------------------------------------+
|139621130423168_23|2023-11-01 01:00:00|139621130423168_09879afe-9d17-435f-8317-28c46ab60888|contact   |{YouTube, ad 4, {88.88.203.156, tammygalloway, NULL}, {Safari, 24.11, LAN, PC, 1.0}}                       |
|139621130423168_34|2023-11-01 01:00:00|139621130423168_26a6620b-b532-4ba0-80d8-f47678185335|category_2|{Medium, ad 5, {223.240.231.131, antonioarmstrong, 2023-10-25 02:00:00}, {Chrome, 24.11, 5G, MacBook, 2.0}}|
|139621130423168_28|2023-11-01 01:00:00|139621130423168_2acd5594-725d-464a-b71a-743cfbd55fbf|page_19   |{NULL, NULL, {2.93.114.134, nicolethompson, NULL}, {Safari, 23.0, Wi-Fi, Smartphone, 3.0}}                 |
|139621130423168_21|2023-11-01 01:00:00|139621130423168_3414ab9d-30ba-4201-85ba-3c4857dcb00d|about     |{YouTube, ad 4, {166.220.148.86, hughesjody, 2023-10-20 02:00:00}, {Safari, 20.0, 5G, MacBook, 6.0}}       |
|139621130423168_38|2023-11-01 01:00:00|139621130423168_36b0be24-1b1f-48ca-bedb-5ace952b713e|category_5|{Google Ads, ad 1, {45.82.19.34, brettlynch, NULL}, {Firefox, 25.09, 5G, Smartphone, 1.0}}                 |
|139621130423168_25|2023-11-01 01:00:00|139621130423168_3b42f2b8-bca4-423e-bb37-f50874f47dbe|home      |{Google Search, ad 5, {180.88.11.239, xbrown, NULL}, {Chrome, 23.1, 4G, iPad, 5.0}}                        |
|139621130423168_20|2023-11-01 01:00:00|139621130423168_40f71080-70dd-4684-a4ba-f3cf29c7ce4d|main      |{StackOverflow, ad 1, {193.147.168.198, landryjames, NULL}, {Chrome, 21.0, Wi-Fi, PC, 5.0}}                |
|139621130423168_48|2023-11-01 01:00:00|139621130423168_485dc82c-939b-4ea3-a1bb-3e77b59e289f|page_15   |{Google Ads, NULL, {75.124.60.112, jamesrobertson, NULL}, {Safari, 20.1, LAN, iPad, 4.0}}                  |
|139621130423168_45|2023-11-01 01:00:00|139621130423168_48e3facb-ba04-4138-abb4-62dfa00107a5|contact   |{YouTube, NULL, {130.184.106.6, samantha74, NULL}, {Firefox, 23.0, Wi-Fi, Smartphone, 5.0}}                |
|139621130423168_33|2023-11-01 01:00:00|139621130423168_4ecfda49-ac01-4f18-88c8-69c18d6ecfc1|home      |{Twitter, ad 4, {131.239.231.211, zevans, NULL}, {Safari, 20.0, Wi-Fi, PC, 3.0}}                           |
|139621130423168_5 |2023-11-01 01:00:00|139621130423168_517a5668-7572-419f-a59b-bd437b267d84|home      |{YouTube, ad 1, {56.71.225.12, joe32, NULL}, {Firefox, 23.1, 4G, PC, 5.0}}                                 |
|139621130423168_44|2023-11-01 01:00:00|139621130423168_547df5cd-2e49-4a6b-90e0-4aff2535e10e|categories|{YouTube, ad 1, {149.215.206.238, ann22, 2023-10-20 02:00:00}, {Chrome, 20.0, LAN, PC, 4.0}}               |
|139621130423168_14|2023-11-01 01:00:00|139621130423168_54c8d612-1202-48f8-b690-91bccd119941|home      |{Google Ads, ad 1, {75.161.252.118, warrenjacqueline, 2023-10-03 02:00:00}, {Firefox, 25.09, 5G, PC, 2.0}} |
|139621130423168_19|2023-11-01 01:00:00|139621130423168_5a159158-f91d-4c6d-9df9-0855717d8bfa|category_2|{NULL, ad 2, {64.84.83.120, shariberry, NULL}, {Safari, 22.0, Wi-Fi, MacBook, 3.0}}                        |
|139621130423168_32|2023-11-01 01:00:00|139621130423168_5eaea3bb-ef5c-4b8d-bf61-c374a38826bb|page_1    |{LinkedIn, NULL, {183.105.213.80, bridgeseric, NULL}, {Chrome, 20.0, Wi-Fi, Smartphone, 6.0}}              |
|139621130423168_29|2023-11-01 01:00:00|139621130423168_63bec493-88d4-4f24-88f0-1bf1dd572121|categories|{Google Ads, NULL, {189.37.14.158, stacey81, NULL}, {Safari, 20.1, 4G, iPad, 2.0}}                         |
|139621130423168_30|2023-11-01 01:00:00|139621130423168_6489958a-46f5-44a9-8a01-24433f03a040|categories|{YouTube, ad 2, {136.31.174.157, jeanette20, 2023-10-06 02:00:00}, {Firefox, 23.2, Wi-Fi, PC, 1.0}}        |
|139621130423168_37|2023-11-01 01:00:00|139621130423168_67b20d55-ebfc-475c-ac76-b38d450f11de|index     |{StackOverflow, NULL, {67.129.64.8, susanevans, NULL}, {Safari, 20.1, LAN, PC, 3.0}}                       |
|139621130423168_18|2023-11-01 01:00:00|139621130423168_6926f263-2443-49a3-9947-116b0989b281|main      |{Facebook, NULL, {132.101.81.179, ecannon, 2023-10-02 02:00:00}, {Chrome, 23.1, 5G, iPad, 5.0}}            |
|139621130423168_24|2023-11-01 01:00:00|139621130423168_69d668b7-7d2c-48ae-a02a-f977e416b01b|about     |{Facebook, NULL, {219.219.64.26, david29, NULL}, {Firefox, 25.09, 5G, MacBook, 3.0}}                       |
+------------------+-------------------+----------------------------------------------------+----------+-----------------------------------------------------------------------------------------------------------+
only showing top 20 rows
```



===================================
1. Explain the [vertical_partitioner_kafka.py](vertical_partitioner_kafka.py)
* the job does the initial vertical partitioning by extracting the user context information from the input raw visits
* later it writes the visit without this context attributes to a _visits_raw_ topic 
* the user attributes go to the _users_context_ topic 
2. Explain the [users_delta_table_creator.py](visits_delta_table_creator.py)
* this simple job creates a new Delta Lake table with the user context
3. Explain the [users_kafka_to_delta_converter.py](users_kafka_to_delta_converter.py)
* the job streams the user context topic and writes it to a Delta user context table with the `MERGE` operation
  so that the table contains only one most recent entry for each user
4. Explain the [users_table_cleaner.py](user_cleaner.py)
* the job removes user data from the user context table
* ⚠️ after this operation, the data is still there. To remove it permanently, you have to run the `VACUUM` command
     that will delete the rows accordingly to the table's retention period

5. Run the `users_delta_table_creator.py` to create the user context table.
6. Start the data generation:
```
cd dataset
docker-compose down --volumes; docker-compose up
```

7. Run the `vertical_partitioner_kafka.py` to split incoming visits.
8. Stop the `vertical_partitioner_kafka.py` after processing all the rows.
9. Run the `users_kafka_to_delta_converter.py` to write data from the user context topic to the equivalent Delta table. 
10. Stop the `users_kafka_to_delta_converter.py` after writing all the rows.
11. Run the `users_table_reader.py`. It should return the list of users, such as:
```
+----------------------------------------------------+---------------+---------------+-------------------+
|user_id                                             |ip             |login          |connected_since    |
+----------------------------------------------------+---------------+---------------+-------------------+
|140665101097856_0316986e-9e7c-448f-9aac-5727dde96537|37.165.220.145 |robertlee      |NULL               |
|140665101097856_04c55690-37cc-4349-86f5-9e616ab2772e|80.73.227.158  |browndenise    |NULL               |
|140665101097856_05ede8fa-7de0-402a-a77f-0bbb09cd3e5e|71.63.127.65   |johnsonpaul    |NULL               |
|140665101097856_1ab59f8e-56d4-407e-a6c6-526f5852d2aa|22.198.58.77   |hernandezsusan |NULL               |
|140665101097856_1c4f2ec4-a815-4c36-a90c-1ae02aa0876c|45.170.191.211 |ischultz       |NULL               |
|140665101097856_1d033a33-55ab-42ee-980e-f92648e1dbc2|89.10.37.251   |mariachen      |NULL               |
|140665101097856_2078add6-c6d4-435f-81f6-833b741a69e5|212.87.171.233 |hernandezryan  |2023-10-27 02:00:00|
|140665101097856_208223bc-2a47-4ea1-9ce3-9607d65f7d8c|117.199.224.101|ryan64         |NULL               |
|140665101097856_344bc942-8737-4048-9798-c88e437fc3d7|35.202.10.73   |matthewwilliams|NULL               |
|140665101097856_391fdb74-550e-488f-9c1e-073e1bb3650a|40.208.40.31   |rebecca85      |2023-10-12 02:00:00|
|140665101097856_3c6bae1b-1bea-4967-89cf-348bcfe31dcf|201.8.220.157  |brittany74     |2023-10-18 02:00:00|
|140665101097856_41798673-00af-4f70-add9-56e1d335b0ca|184.223.238.239|khernandez     |NULL               |
|140665101097856_44ecfed9-c4b5-446f-a9a1-0e41e78b32ac|221.122.15.138 |sarah20        |NULL               |
|140665101097856_4946b457-b5fe-411b-af2f-addb7af610f8|119.64.226.132 |travisturner   |2023-10-11 02:00:00|
|140665101097856_4b3ebe03-1a2c-439d-878e-0764aa7c6d7c|97.3.167.118   |zkemp          |2023-10-10 02:00:00|
|140665101097856_4b88a566-4bba-4389-a77c-96ac40d0f469|58.177.144.144 |hamiltonbrandon|2023-10-12 02:00:00|
|140665101097856_4e722d1f-5c75-4afd-8474-a847582f4db5|188.204.113.99 |michaelbaker   |NULL               |
|140665101097856_4fe2ba63-2208-41fd-84a3-056fd6d8c379|133.11.145.27  |ogomez         |NULL               |
|140665101097856_5acea2d3-66b6-4a42-8a1d-209386a91eb1|39.232.88.2    |zconway        |NULL               |
|140665101097856_63707fe0-e00e-4288-b47c-3244dd77c689|53.213.90.182  |hmartin        |2023-10-12 02:00:00|
+----------------------------------------------------+---------------+---------------+-------------------+
only showing top 20 rows
```

12. Set the `user_id_to_delete` variable in `users_table_cleaner.py` to one of the read users and run the job.
13. Run the `users_table_reader.py`. It should show all users but the one deleted before:
```
# 140665101097856_0316986e-9e7c-448f-9aac-5727dde96537 should be missing compared to the previous snippet
+----------------------------------------------------+---------------+---------------+-------------------+
|user_id                                             |ip             |login          |connected_since    |
+----------------------------------------------------+---------------+---------------+-------------------+
|140665101097856_04c55690-37cc-4349-86f5-9e616ab2772e|80.73.227.158  |browndenise    |NULL               |
|140665101097856_05ede8fa-7de0-402a-a77f-0bbb09cd3e5e|71.63.127.65   |johnsonpaul    |NULL               |
|140665101097856_1ab59f8e-56d4-407e-a6c6-526f5852d2aa|22.198.58.77   |hernandezsusan |NULL               |
|140665101097856_1c4f2ec4-a815-4c36-a90c-1ae02aa0876c|45.170.191.211 |ischultz       |NULL               |
|140665101097856_1d033a33-55ab-42ee-980e-f92648e1dbc2|89.10.37.251   |mariachen      |NULL               |
|140665101097856_2078add6-c6d4-435f-81f6-833b741a69e5|212.87.171.233 |hernandezryan  |2023-10-27 02:00:00|
|140665101097856_208223bc-2a47-4ea1-9ce3-9607d65f7d8c|117.199.224.101|ryan64         |NULL               |
|140665101097856_344bc942-8737-4048-9798-c88e437fc3d7|35.202.10.73   |matthewwilliams|NULL               |
|140665101097856_391fdb74-550e-488f-9c1e-073e1bb3650a|40.208.40.31   |rebecca85      |2023-10-12 02:00:00|
|140665101097856_3c6bae1b-1bea-4967-89cf-348bcfe31dcf|201.8.220.157  |brittany74     |2023-10-18 02:00:00|
|140665101097856_41798673-00af-4f70-add9-56e1d335b0ca|184.223.238.239|khernandez     |NULL               |
|140665101097856_44ecfed9-c4b5-446f-a9a1-0e41e78b32ac|221.122.15.138 |sarah20        |NULL               |
|140665101097856_4946b457-b5fe-411b-af2f-addb7af610f8|119.64.226.132 |travisturner   |2023-10-11 02:00:00|
|140665101097856_4b3ebe03-1a2c-439d-878e-0764aa7c6d7c|97.3.167.118   |zkemp          |2023-10-10 02:00:00|
|140665101097856_4b88a566-4bba-4389-a77c-96ac40d0f469|58.177.144.144 |hamiltonbrandon|2023-10-12 02:00:00|
|140665101097856_4e722d1f-5c75-4afd-8474-a847582f4db5|188.204.113.99 |michaelbaker   |NULL               |
|140665101097856_4fe2ba63-2208-41fd-84a3-056fd6d8c379|133.11.145.27  |ogomez         |NULL               |
|140665101097856_5acea2d3-66b6-4a42-8a1d-209386a91eb1|39.232.88.2    |zconway        |NULL               |
|140665101097856_63707fe0-e00e-4288-b47c-3244dd77c689|53.213.90.182  |hmartin        |2023-10-12 02:00:00|
|140665101097856_65fb5d0f-7841-4417-9d8c-48fefd6675e3|16.7.96.208    |jessica78      |2023-10-27 02:00:00|
+----------------------------------------------------+---------------+---------------+-------------------+
only showing top 20 rows
```

14. Read all user context and look for the removed user id:
```
$ docker exec dedp_kafka kafka-console-consumer.sh --topic users_context --bootstrap-server localhost:9092 --from-beginning --property parse.key=true --property print.key=true

140665101097856_1ab59f8e-56d4-407e-a6c6-526f5852d2aa	{"ip":"22.198.58.77","login":"hernandezsusan","user_id":"140665101097856_1ab59f8e-56d4-407e-a6c6-526f5852d2aa"}
140665101097856_cb1fd766-d69c-4270-b9b5-45d9e6850958	{"ip":"63.213.75.17","login":"slong","user_id":"140665101097856_cb1fd766-d69c-4270-b9b5-45d9e6850958"}
140665101097856_bcbcae25-78ac-4a9f-b403-81d0b3cdaa00	{"ip":"115.212.177.82","login":"pjensen","user_id":"140665101097856_bcbcae25-78ac-4a9f-b403-81d0b3cdaa00"}

--> 140665101097856_0316986e-9e7c-448f-9aac-5727dde96537	{"ip":"37.165.220.145","login":"robertlee","user_id":"140665101097856_0316986e-9e7c-448f-9aac-5727dde96537"}

140665101097856_391fdb74-550e-488f-9c1e-073e1bb3650a	{"ip":"40.208.40.31","login":"rebecca85","connected_since":"2023-10-12T02:00:00.000+02:00","user_id":"140665101097856_391fdb74-550e-488f-9c1e-073e1bb3650a"}
140665101097856_f66f3f69-1fd7-4789-9e6c-04107eeb2a28	{"ip":"170.142.164.20","login":"james93","user_id":"140665101097856_f66f3f69-1fd7-4789-9e6c-04107eeb2a28"}
140665101097856_4fe2ba63-2208-41fd-84a3-056fd6d8c379	{"ip":"133.11.145.27","login":"ogomez","user_id":"140665101097856_4fe2ba63-2208-41fd-84a3-056fd6d8c379"}
140665101097856_04c55690-37cc-4349-86f5-9e616ab2772e	{"ip":"80.73.227.158","login":"browndenise","user_id":"140665101097856_04c55690-37cc-4349-86f5-9e616ab2772e"}
```



15. Clean the user in the Apache Kafka topic by producing an empty value for the deleted user id, plus some other
records to trigger the ocmpaction:
```
docker exec -ti dedp_kafka  kafka-console-producer.sh  --bootstrap-server localhost:9094 --topic users_context --property parse.key=true --property key.separator=,

140665101097856_0316986e-9e7c-448f-9aac-5727dde96537,{}
a,a
b,b
c,c
d,d
```

15. Wait for the cleaning logs to be printed and see if the user context also disappeared from the Kafka topic:
```
$ docker exec dedp_kafka kafka-console-consumer.sh --topic users_context --bootstrap-server localhost:9092 --from-beginning --property parse.key=true --property print.key=true

# ...
140665101097856_967aa95d-50e4-4951-b8b6-f9987c77dea0	{"ip":"139.193.222.80","login":"tammyvilla","connected_since":"2023-10-12T02:00:00.000+02:00","user_id":"140665101097856_967aa95d-50e4-4951-b8b6-f9987c77dea0"}
140665101097856_bb6dd75b-f516-405d-ade2-f7cde59242f4	{"ip":"79.147.229.41","login":"devin65","connected_since":"2023-10-14T02:00:00.000+02:00","user_id":"140665101097856_bb6dd75b-f516-405d-ade2-f7cde59242f4"}
140665101097856_4e722d1f-5c75-4afd-8474-a847582f4db5	{"ip":"188.204.113.99","login":"michaelbaker","user_id":"140665101097856_4e722d1f-5c75-4afd-8474-a847582f4db5"}
140665101097856_bcc923e8-f8ea-4084-8d63-74b03f671743	{"ip":"208.188.231.4","login":"ahartman","user_id":"140665101097856_bcc923e8-f8ea-4084-8d63-74b03f671743"}
140665101097856_05ede8fa-7de0-402a-a77f-0bbb09cd3e5e	{"ip":"71.63.127.65","login":"johnsonpaul","user_id":"140665101097856_05ede8fa-7de0-402a-a77f-0bbb09cd3e5e"}

--> 140665101097856_0316986e-9e7c-448f-9aac-5727dde96537	{}

d	d
a	a
b	b
c	c
```

The cleaning logs should look like that:
```
INFO Cleaner 0: Cleaning log users_context-0 (cleaning prior to Wed May 15 03:35:47 UTC 2024, discarding tombstones prior to upper bound deletion horizon Tue May 14 03:13:44 UTC 2024)... (kafka.log.LogCleaner)
INFO Cleaner 0: Cleaning LogSegment(baseOffset=0, size=4481, ....,retaining deletes. (kafka.log.LogCleaner)
INFO [kafka-log-cleaner-thread-0]: 
	Log cleaner thread 0 cleaned log users_context-0 (dirty section = [240, 241])
	0.0 MB of log processed in 0.0 seconds (0.1 MB/sec).
	Indexed 0.0 MB in 0.0 seconds (0.0 Mb/sec, 67.6% of total time)
	Buffer utilization: 0.0%
	Cleaned 0.0 MB in 0.0 seconds (0.4 Mb/sec, 32.4% of total time)
	Start size: 0.0 MB (25 messages)
	End size: 0.0 MB (24 messages)
	3.7% size reduction (4.0% fewer messages)
 (kafka.log.LogCleaner)
```