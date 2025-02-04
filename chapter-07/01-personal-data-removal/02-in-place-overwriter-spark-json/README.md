# Data removal - in-place overwriter with Apache Spark and JSON

1. Generate the test data:
```
cd dataset
mkdir -p /tmp/dedp/ch07/01-personal-data-removal/02-in-place-overwrite-spark-json/input
docker-compose down --volumes; docker-compose up
```
2. Run the `bootstrap_output_table.py` to copy the input data to the output location.
3. Explain the [user_cleaner.py](user_cleaner.py)
* the job filters out the rows with the removed user id
* the schema only contains the user_id attribute to reduce the storage footprint
* besides, the job writes the removed results into a staging location and copies them only when the job
  succeeds to the output location in the end
4. Run the `visits_table_reader.py` and chose one user to remove (e.g. 139621130423168_029fba78-15dc-4944-9f65-00636566f75b)
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
only showing top 20 row
```

5. Run the `user_cleaner.py`.
6. Run the `visits_table_reader.py`. You shouldn't see the removed user in the list
```
# removed 139621130423168_029fba78-15dc-4944-9f65-00636566f75b

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