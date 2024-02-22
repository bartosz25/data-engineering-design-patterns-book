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