1. Generate the dataset:
```
cd dataset
/tmp/dedp/ch05/02-decorator/01-wrapper-sql/input/
docker-compose down --volumes; docker-compose up
```
2. Explain the [visits_decorator.py](visits_decorator.py)
* it's the data decoration job that implements three different decoration formats:
  * decorated values as a separated structure in a single column, raw data as columns
  * decorated values as columns, raw data as a structure
  * decorated values and raw data as columns
* it leverages the `STRUCT` data type that greatly represents complex types in a column
3. Run the `visits_decorator.py`
4. Run the `visits_table_reader.py`
It should print the decorated rows:
```
========= STRUCT DECORATED =========
24/02/22 06:29:09 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
+-----------------+-------------------+----------------------------------------------------+----------+----------------------------------------------------------------------------------------+---------------------------------+
|visit_id         |event_time         |user_id                                             |page      |context                                                                                 |decorated                        |
+-----------------+-------------------+----------------------------------------------------+----------+----------------------------------------------------------------------------------------+---------------------------------+
|140305711573888_0|2023-11-06 01:00:00|140305711573888_e33ac2ac-b66a-4a0f-b6a5-90d0842115f0|categories|{StackOverflow, ad 1, {9.244.237.140, uwilliams, NULL}, {Safari, 25.09, LAN, iPad, 1.0}}|{false, categories-StackOverflow}|
+-----------------+-------------------+----------------------------------------------------+----------+----------------------------------------------------------------------------------------+---------------------------------+
only showing top 1 row

========= STRUCT RAW =========
+------------+------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|is_connected|page_referral_key       |raw                                                                                                                                                                                                 |
+------------+------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|false       |categories-StackOverflow|{140305711573888_0, 2023-11-06 01:00:00, 140305711573888_e33ac2ac-b66a-4a0f-b6a5-90d0842115f0, categories, {StackOverflow, ad 1, {9.244.237.140, uwilliams, NULL}, {Safari, 25.09, LAN, iPad, 1.0}}}|
+------------+------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
only showing top 1 row

========= FLATTEN =========
+-----------------+-------------------+----------------------------------------------------+----------+----------------------------------------------------------------------------------------+------------+------------------------+
|visit_id         |event_time         |user_id                                             |page      |context                                                                                 |is_connected|page_referral_key       |
+-----------------+-------------------+----------------------------------------------------+----------+----------------------------------------------------------------------------------------+------------+------------------------+
|140305711573888_0|2023-11-06 01:00:00|140305711573888_e33ac2ac-b66a-4a0f-b6a5-90d0842115f0|categories|{StackOverflow, ad 1, {9.244.237.140, uwilliams, NULL}, {Safari, 25.09, LAN, iPad, 1.0}}|false       |categories-StackOverflow|
+-----------------+-------------------+----------------------------------------------------+----------+----------------------------------------------------------------------------------------+------------+------------------------+
only showing top 1 row
```









2. Load the dataset to the Delta Lake table by running the [load_devices_data.py](visits_decorator.py)
3. Repeat the steps 1-2 4 times.
4. Check the Delta table output. You should see many small files, prefixed by `--->` in the output below.
```
ls -lh /tmp/dedp/ch02/data-compactor/devices-table/
total 20K
4.0K Dec 24 11:29 _delta_log
---> 2.1K Dec 24 11:29 part-00000-35d448a1-f927-47aa-8583-1aeff939342a-c000.snappy.parquet
---> 2.0K Dec 24 11:26 part-00000-44cd9ba7-6b68-4857-95c0-bfb337ac24fa-c000.snappy.parquet
---> 2.2K Dec 24 11:28 part-00000-635ac8ca-c376-4ec2-9477-7456879f714b-c000.snappy.parquet
---> 2.1K Dec 24 11:28 part-00000-cfb0ff50-c11f-49e8-8dcc-de19ade9e59e-c000.snappy.parquet
```
5. Open the compaction job [compact_devices_table.py](compact_devices_table.py)
* it calls a maintenance operation `executeCompaction()`
6. Run the `compact_devices_table.py`
7. Check the output. You should see small file merged into bigger ones by the compaction job. The merge result 
is prefixed with `--->` in the output below:
```
ls -lh /tmp/dedp/ch02/data-compactor/devices-table/

4.0K Dec 24 11:34 _delta_log
2.1K Dec 24 11:29 part-00000-35d448a1-f927-47aa-8583-1aeff939342a-c000.snappy.parquet
2.0K Dec 24 11:26 part-00000-44cd9ba7-6b68-4857-95c0-bfb337ac24fa-c000.snappy.parquet
2.2K Dec 24 11:28 part-00000-635ac8ca-c376-4ec2-9477-7456879f714b-c000.snappy.parquet
---> 2.7K Dec 24 11:34 part-00000-95ce4660-827a-4d9e-aa10-2d6d15c23a09-c000.snappy.parquet
2.1K Dec 24 11:28 part-00000-cfb0ff50-c11f-49e8-8dcc-de19ade9e59e-c000.snappy.parquet
```
8. As you can see, the small files are still there. To get rid of them, you need to run a `VACUUM` operation.
   The execution is conditioned by a retention configuration, so to overcome it, change the date on your PC 
   to `today + 32 days` and run the `vacuum_devices_table.py`.
* the job uses another maintenance 
9. Check the output. You should see small files removed. 
```
ls -lh /tmp/dedp/ch02/data-compactor/devices-table/
total 8.0K
4.0K Jan 27 11:36 _delta_log
2.7K Dec 24 11:34 part-00000-95ce4660-827a-4d9e-aa10-2d6d15c23a09-c000.snappy.parquet
```
and a message like:
> Deleted 4 files and directories in a total of 1 directories.
 
⚠️ Once the compacted files removed, you cannot run a _time travel_ on them anymore.