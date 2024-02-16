# Data combination - distributed combinator with Apache Spark for JSON and PostgreSQL datasets

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch05/03-data-combination/01-distributed-json-postgresql/visits
docker-compose down --volumes; docker-compose up
```
2. Explain the [distributed_combiner.py](distributed_combiner.py)
* the code joins JSON dataset with PostgreSQL dataset
  * they're two physically separated datasets; hence combining them locally is not possible as by definition
    they don't live in the same storage
3. Run the `distributed_combiner.py`
As a result you'll first see that the execution plan with 
* different `Scan` operations
* an `Exchange` for the network-based data combination (aka shuffle)
```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- SortMergeJoin [context#8.technical.device_type, context#8.technical.device_version], [type#20, version#22], Inner
   :- Sort [context#8.technical.device_type ASC NULLS FIRST, context#8.technical.device_version ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(context#8.technical.device_type, context#8.technical.device_version, 200), ENSURE_REQUIREMENTS, [plan_id=21]
   :     +- Filter (isnotnull(context#8.technical.device_type) AND isnotnull(context#8.technical.device_version))
   :        +- FileScan json [context#8,event_time#9,keep_private#10,page#11,user_id#12,visit_id#13] Batched: false, DataFilters: [isnotnull(context#8.technical.device_type), isnotnull(context#8.technical.device_version)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/dedp/ch05/03-data-combination/01-distributed-json-postgresql..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<context:struct<ad_id:string,referral:string,technical:struct<browser:string,browser_versio...
   +- Sort [type#20 ASC NULLS FIRST, version#22 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(type#20, version#22, 200), ENSURE_REQUIREMENTS, [plan_id=22]
         +- Scan JDBCRelation(dedp.devices) [numPartitions=1] [type#20,full_name#21,version#22] PushedFilters: [*IsNotNull(type), *IsNotNull(version)], ReadSchema: struct<type:string,full_name:string,version:string>
```

Next, you'll see a snippet of the join result:
```
+--------------------------------------------------------------------------------------------------------------+-------------------------+------------+-------+----------------------------------------------------+-------------------+------+-------------+----------+
|context                                                                                                       |event_time               |keep_private|page   |user_id                                             |visit_id           |type  |full_name    |version   |
+--------------------------------------------------------------------------------------------------------------+-------------------------+------------+-------+----------------------------------------------------+-------------------+------+-------------+----------+
|{ad 3, LinkedIn, {Safari, 22.0, galaxy, Android 10, 5G}, {NULL, 143.139.207.199, murraydaniel}}               |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_757129e0-ef20-4ba3-91e1-e26c289c1023|140469483903872_11 |galaxy|Galaxy Q     |Android 10|
|{ad 3, LinkedIn, {Safari, 22.0, galaxy, Android 10, 5G}, {NULL, 143.139.207.199, murraydaniel}}               |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_757129e0-ef20-4ba3-91e1-e26c289c1023|140469483903872_11 |galaxy|Galaxy Gio   |Android 10|
|{ad 3, LinkedIn, {Safari, 22.0, galaxy, Android 10, 5G}, {NULL, 143.139.207.199, murraydaniel}}               |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_757129e0-ef20-4ba3-91e1-e26c289c1023|140469483903872_11 |galaxy|Galaxy S Plus|Android 10|
|{ad 3, LinkedIn, {Safari, 22.0, galaxy, Android 10, 5G}, {NULL, 143.139.207.199, murraydaniel}}               |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_757129e0-ef20-4ba3-91e1-e26c289c1023|140469483903872_11 |galaxy|Galaxy Camera|Android 10|
|{ad 3, LinkedIn, {Safari, 22.0, galaxy, Android 10, 5G}, {NULL, 143.139.207.199, murraydaniel}}               |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_757129e0-ef20-4ba3-91e1-e26c289c1023|140469483903872_11 |galaxy|Galaxy Ace   |Android 10|
|{ad 3, LinkedIn, {Safari, 22.0, galaxy, Android 10, 5G}, {NULL, 143.139.207.199, murraydaniel}}               |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_757129e0-ef20-4ba3-91e1-e26c289c1023|140469483903872_11 |galaxy|Galaxy W     |Android 10|
|{NULL, NULL, {Firefox, 23.0, galaxy, Android 10, 5G}, {NULL, 195.34.146.28, ssanders}}                        |2023-11-24T00:00:00+00:00|false       |page_16|140469483903872_ff066ff8-a6f1-4ef5-8c7a-b8dfa51a14b7|140469483903872_84 |galaxy|Galaxy Q     |Android 10|
|{NULL, NULL, {Firefox, 23.0, galaxy, Android 10, 5G}, {NULL, 195.34.146.28, ssanders}}                        |2023-11-24T00:00:00+00:00|false       |page_16|140469483903872_ff066ff8-a6f1-4ef5-8c7a-b8dfa51a14b7|140469483903872_84 |galaxy|Galaxy Gio   |Android 10|
|{NULL, NULL, {Firefox, 23.0, galaxy, Android 10, 5G}, {NULL, 195.34.146.28, ssanders}}                        |2023-11-24T00:00:00+00:00|false       |page_16|140469483903872_ff066ff8-a6f1-4ef5-8c7a-b8dfa51a14b7|140469483903872_84 |galaxy|Galaxy S Plus|Android 10|
|{NULL, NULL, {Firefox, 23.0, galaxy, Android 10, 5G}, {NULL, 195.34.146.28, ssanders}}                        |2023-11-24T00:00:00+00:00|false       |page_16|140469483903872_ff066ff8-a6f1-4ef5-8c7a-b8dfa51a14b7|140469483903872_84 |galaxy|Galaxy Camera|Android 10|
|{NULL, NULL, {Firefox, 23.0, galaxy, Android 10, 5G}, {NULL, 195.34.146.28, ssanders}}                        |2023-11-24T00:00:00+00:00|false       |page_16|140469483903872_ff066ff8-a6f1-4ef5-8c7a-b8dfa51a14b7|140469483903872_84 |galaxy|Galaxy Ace   |Android 10|
|{NULL, NULL, {Firefox, 23.0, galaxy, Android 10, 5G}, {NULL, 195.34.146.28, ssanders}}                        |2023-11-24T00:00:00+00:00|false       |page_16|140469483903872_ff066ff8-a6f1-4ef5-8c7a-b8dfa51a14b7|140469483903872_84 |galaxy|Galaxy W     |Android 10|
|{ad 3, Google Ads, {Safari, 23.0, galaxy, Android 10, 5G}, {NULL, 200.28.218.206, lee28}}                     |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_aca60a3a-7127-44b0-a670-218aecf160d9|140469483903872_115|galaxy|Galaxy Q     |Android 10|
|{ad 3, Google Ads, {Safari, 23.0, galaxy, Android 10, 5G}, {NULL, 200.28.218.206, lee28}}                     |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_aca60a3a-7127-44b0-a670-218aecf160d9|140469483903872_115|galaxy|Galaxy Gio   |Android 10|
|{ad 3, Google Ads, {Safari, 23.0, galaxy, Android 10, 5G}, {NULL, 200.28.218.206, lee28}}                     |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_aca60a3a-7127-44b0-a670-218aecf160d9|140469483903872_115|galaxy|Galaxy S Plus|Android 10|
|{ad 3, Google Ads, {Safari, 23.0, galaxy, Android 10, 5G}, {NULL, 200.28.218.206, lee28}}                     |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_aca60a3a-7127-44b0-a670-218aecf160d9|140469483903872_115|galaxy|Galaxy Camera|Android 10|
|{ad 3, Google Ads, {Safari, 23.0, galaxy, Android 10, 5G}, {NULL, 200.28.218.206, lee28}}                     |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_aca60a3a-7127-44b0-a670-218aecf160d9|140469483903872_115|galaxy|Galaxy Ace   |Android 10|
|{ad 3, Google Ads, {Safari, 23.0, galaxy, Android 10, 5G}, {NULL, 200.28.218.206, lee28}}                     |2023-11-24T00:00:00+00:00|false       |about  |140469483903872_aca60a3a-7127-44b0-a670-218aecf160d9|140469483903872_115|galaxy|Galaxy W     |Android 10|
|{NULL, YouTube, {Firefox, 21.0, galaxy, Android 10, LAN}, {2023-11-23T00:00:00+00:00, 108.25.141.202, zortiz}}|2023-11-24T00:00:00+00:00|false       |page_17|140469483903872_9ef2d18f-2cad-4a59-bf4e-f84fd0fb89a8|140469483903872_137|galaxy|Galaxy Q     |Android 10|
|{NULL, YouTube, {Firefox, 21.0, galaxy, Android 10, LAN}, {2023-11-23T00:00:00+00:00, 108.25.141.202, zortiz}}|2023-11-24T00:00:00+00:00|false       |page_17|140469483903872_9ef2d18f-2cad-4a59-bf4e-f84fd0fb89a8|140469483903872_137|galaxy|Galaxy Gio   |Android 10|
+--------------------------------------------------------------------------------------------------------------+-------------------------+------------+-------+----------------------------------------------------+-------------------+------+-------------+----------+
only showing top 20 rows
```