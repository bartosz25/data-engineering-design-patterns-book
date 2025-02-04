# Data removal - vertical partitioner with Apache Spark, Apache Kafka, and Delta Lake

1. Explain the [vertical_partitioner_kafka.py](vertical_partitioner_kafka.py)
* the job does the initial vertical partitioning by extracting the user context information from the input raw visits
* later it writes the visit without this context attributes to a _visits_raw_ topic 
* the user attributes go to the _users_context_ topic 
2. Explain the [users_delta_table_creator.py](users_delta_table_creator.py)
* this simple job creates a new Delta Lake table with the user context
3. Explain the [users_kafka_to_delta_converter.py](users_kafka_to_delta_converter.py)
* the job streams the user context topic and writes it to a Delta user context table with the `MERGE` operation
  so that the table contains only one most recent entry for each user
4. Explain the [users_table_cleaner.py](users_table_cleaner.py)
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
8. Stop the `vertical_partitioner_kafka.py` after processing all the rows, once you see this message for the started containers
_dataset-data_generator-1 exited with code 0_.
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
records to trigger the compaction:
```
docker exec -ti dedp_kafka  kafka-console-producer.sh  --bootstrap-server localhost:9094 --topic users_context --property parse.key=true --property key.separator=, --property null.marker=NULL

140665101097856_0316986e-9e7c-448f-9aac-5727dde96537,NULL
a,a
b,b
c,c
d,d
ss,s
ff,g
eo,f
ccccc,b
za,fo
v,vnv
eiei,e
cxpcvxc,vc
iep,fs
b,b
hh,h
eoe,e
dqq,ff
pe,tg
cxz,ree
cxpx,x
```

15. Wait for the cleaning logs to be printed and see if the user context also disappeared from the Kafka topic:
```
$ docker exec dedp_kafka kafka-console-consumer.sh --topic users_context --bootstrap-server localhost:9092 --from-beginning --property parse.key=true --property print.key=true | less | grep '140665101097856_0316986e-9e7c-448f-9aac-5727dde96537'
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
 
INFO [UnifiedLog partition=users_context-0, dir=/bitnami/kafka/data] Deleting segment files LogSegment(baseOffset=106, size=77, lastModifiedTime=1717554788602, largestRecordTimestamp=Some(1717554787596)) (kafka.log.LocalLog$)
INFO [UnifiedLog partition=users_context-0, dir=/bitnami/kafka/data] Deleting segment files LogSegment(baseOffset=0, size=4413, lastModifiedTime=1717554788602, largestRecordTimestamp=Some(1717554783016)) (kafka.log.LocalLog$)
INFO Deleted log /bitnami/kafka/data/users_context-0/00000000000000000106.log.deleted. (kafka.log.LogSegment)
INFO Deleted log /bitnami/kafka/data/users_context-0/00000000000000000000.log.deleted. (kafka.log.LogSegment)
INFO Deleted offset index /bitnami/kafka/data/users_context-0/00000000000000000106.index.deleted. (kafka.log.LogSegment)
INFO Deleted time index /bitnami/kafka/data/users_context-0/00000000000000000106.timeindex.deleted. (kafka.log.LogSegment)
INFO Deleted offset index /bitnami/kafka/data/users_context-0/00000000000000000000.index.deleted. (kafka.log.LogSegment)
INFO Deleted time index /bitnami/kafka/data/users_context-0/00000000000000000000.timeindex.deleted. (kafka.log.LogSegment)
```