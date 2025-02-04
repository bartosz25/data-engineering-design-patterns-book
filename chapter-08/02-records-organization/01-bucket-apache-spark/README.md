# Records organization - buckets with Apache Spark

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch08/01-records-organization/01-buckets-apache-spark/input-users
mkdir -p /tmp/dedp/ch08/01-records-organization/01-buckets-apache-spark/input-devices
mkdir -p /tmp/dedp/ch08/01-records-organization/01-buckets-apache-spark/input-visits
docker-compose down --volumes; docker-compose up
```
2. Explain the [bucket_preparators.py](bucket_preparators.py)
* the code creates _bucketed_ tables, i.e. tables where related keys are stored together
  * if the two tables are bucketed on the join key and have the same number of buckets, the join will be local
3. Run the `bucket_preparators.py`
4. Explain the [local_combiner.py](local_combiner.py)
* the code joins bucketed tables first and prints the execution plan
* later, the second join is made between a bucketed and non-bucketed table; here too, it prints the execution plan
5. Run the `local_combiner.py`
As a result you'll see that the execution plan for the bucketed tables doesn't have any `Exchange` node 
meaning there is no shuffle. 
```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [visit_id#5, user_id#4, login#10]
   +- SortMergeJoin [user_id#4], [id#8], Inner
      :- Sort [user_id#4 ASC NULLS FIRST], false, 0
      :  +- Filter isnotnull(user_id#4)
      :     +- FileScan parquet spark_catalog.default.visits[user_id#4,visit_id#5] Batched: true, Bucketed: true, DataFilters: [isnotnull(user_id#4)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/dedp/ch05/03-data-combination/02-local-buckets-spark/warehou..., PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,visit_id:string>, SelectedBucketsCount: 5 out of 5
      +- Sort [id#8 ASC NULLS FIRST], false, 0
         +- Filter isnotnull(id#8)
            +- FileScan parquet spark_catalog.default.users[id#8,login#10] Batched: true, Bucketed: true, DataFilters: [isnotnull(id#8)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/dedp/ch05/03-data-combination/02-local-buckets-spark/warehou..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:string,login:string>, SelectedBucketsCount: 5 out of 5
```

It's not the case of the second join where the shuffle happens before combining both tables:
```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [visit_id#5, user_id#4, full_name#17]
   +- SortMergeJoin [_extract_device_type#26, _extract_device_version#27], [type#18, version#19], Inner
      :- Sort [_extract_device_type#26 ASC NULLS FIRST, _extract_device_version#27 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(_extract_device_type#26, _extract_device_version#27, 200), ENSURE_REQUIREMENTS, [plan_id=67]
      :     +- Project [context#0.technical.device_type AS _extract_device_type#26, context#0.technical.device_version AS _extract_device_version#27, user_id#4, visit_id#5]
      :        +- Filter (isnotnull(context#0.technical.device_type) AND isnotnull(context#0.technical.device_version))
      :           +- FileScan parquet spark_catalog.default.visits[context#0,user_id#4,visit_id#5] Batched: true, Bucketed: false (disabled by query planner), DataFilters: [isnotnull(context#0.technical.device_type), isnotnull(context#0.technical.device_version)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/dedp/ch05/03-data-combination/02-local-buckets-spark/warehou..., PartitionFilters: [], PushedFilters: [IsNotNull(context.technical.device_type), IsNotNull(context.technical.device_version)], ReadSchema: struct<context:struct<technical:struct<device_type:string,device_version:string>>,user_id:string,...
      +- Sort [type#18 ASC NULLS FIRST, version#19 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(type#18, version#19, 200), ENSURE_REQUIREMENTS, [plan_id=59]
            +- Filter (isnotnull(type#18) AND isnotnull(version#19))
               +- FileScan parquet spark_catalog.default.devices[full_name#17,type#18,version#19] Batched: true, DataFilters: [isnotnull(type#18), isnotnull(version#19)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/dedp/ch05/03-data-combination/02-local-buckets-spark/warehou..., PartitionFilters: [], PushedFilters: [IsNotNull(type), IsNotNull(version)], ReadSchema: struct<full_name:string,type:string,version:string>
```

6. Explain [bucket_appender.py](bucket_appender.py)
* the code is similar to the `bucket_preparators.py` but it changes the bucketing column for the visits dataset
  to _visit_id_
7. Run the `bucket_appender.py`. It should fail with an error:

```
pyspark.errors.exceptions.captured.AnalysisException: Specified bucketing does not match that of the existing table spark_catalog.default.visits.
Specified bucketing: 5 buckets, bucket columns: [visit_id].
Existing bucketing: 5 buckets, bucket columns: [user_id].
```

It proves that the buckets are immutable and cannot be changed without overwriting the existing dataset. In other
words, a table doesn't support two different bucket schemas.