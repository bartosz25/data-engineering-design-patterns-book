# Schema versioner - Delta Lake with Apache Spark Structured Streaming

1. Explain the [create_table.py](create_table.py)
* the job creates a new _visits_ table
* an important thing here is the `'delta.columnMapping.mode' = 'name'` option that enables support for schema versioning
2. Run `create_table.py`. You should get:
```
+----------------------------+-------------------------------------------------------------------------------------------+
|name                        |tableFeatures                                                                              |
+----------------------------+-------------------------------------------------------------------------------------------+
|spark_catalog.default.visits|[appendOnly, changeDataFeed, checkConstraints, columnMapping, generatedColumns, invariants]|
+----------------------------+-------------------------------------------------------------------------------------------+

+-------------------------------+-----+
|key                            |value|
+-------------------------------+-----+
|delta.columnMapping.maxColumnId|4    |
|delta.columnMapping.mode       |name |
|delta.minReaderVersion         |2    |
|delta.minWriterVersion         |5    |
+-------------------------------+-----+

+--------+----------------------+-------+----------+
|visit_id|event_time            |user_id|page      |
+--------+----------------------+-------+----------+
|visit 1 |2024-07-01 12:00:00.84|user 1 |index.html|
+--------+----------------------+-------+----------+
```

3. Explain [start_streaming_and_table_rename.py](start_streaming_and_table_rename.py)
* the code starts a simple streaming job that prints the content of the table 
  * an important thing here is the `spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop` flag
  set to _always_ at the `SparkSession` level; it enables support for the column renames and removals in the streaming job
  * besides the job uses a `schemaTrackingLocation` option where Apache Spark will store the mapping for 
  renamed or removed columns
* just after running the job, we're renaming the column, adding some data, and expecting the streaming job
to print something on the screen again

4. Run `start_streaming_and_table_rename.py`
As you can see, the job failed after the renaming step:

```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------+----------------------+-------+----------+
|visit_id|event_time            |user_id|page      |
+--------+----------------------+-------+----------+
|visit 1 |2024-07-01 12:00:00.84|user 1 |index.html|
+--------+----------------------+-------+----------+

-------------------------------------------
Batch: 1
-------------------------------------------
+--------+----------+-------+----+
|visit_id|event_time|user_id|page|
+--------+----------+-------+----+
+--------+----------+-------+----+
+----------------------------+-------------------------------------------------------------------------------------------+
|name                        |tableFeatures                                                                              |
+----------------------------+-------------------------------------------------------------------------------------------+
|spark_catalog.default.visits|[appendOnly, changeDataFeed, checkConstraints, columnMapping, generatedColumns, invariants]|
+----------------------------+-------------------------------------------------------------------------------------------+

+-------------------------------+-----+
|key                            |value|
+-------------------------------+-----+
|delta.columnMapping.maxColumnId|4    |
|delta.columnMapping.mode       |name |
|delta.minReaderVersion         |2    |
|delta.minWriterVersion         |5    |
+-------------------------------+-----+

24/08/06 20:40:48 ERROR MicroBatchExecution: Query [id = f66e6565-1ad2-46f0-b176-19de79dcb9d9, runId = 4a4d85a0-c1c6-46d7-aeb0-ef4a8c04c1f4] terminated with error
org.apache.spark.sql.delta.DeltaRuntimeException: [DELTA_STREAMING_METADATA_EVOLUTION] The schema, table configuration or protocol of your Delta table has changed during streaming.
The schema or metadata tracking log has been updated.
Please restart the stream to continue processing using the updated metadata.
Updated schema: root
 |-- visit_id: string (nullable = false)
 |-- event_time: timestamp (nullable = false)
 |-- user: string (nullable = false)
 |-- page: string (nullable = false)
.
Updated table configurations: delta.columnMapping.mode:name, delta.columnMapping.maxColumnId:4.
Updated table protocol: 2,5
	at org.apache.spark.sql.delta.DeltaErrorsBase.streamingMetadataEvolutionException(DeltaErrors.scala:2785)
	at org.apache.spark.sql.delta.DeltaErrorsBase.streamingMetadataEvolutionException$(DeltaErrors.scala:2774)
	at org.apache.spark.sql.delta.DeltaErrors$.streamingMetadataEvolutionException(DeltaErrors.scala:3039)
	at org.apache.spark.sql.delta.sources.DeltaSourceMetadataEvolutionSupport.updateMetadataTrackingLogAndFailTheStreamIfNeeded(DeltaSourceMetadataEvolutionSupport.scala:423)
	at org.apache.spark.sql.delta.sources.DeltaSourceMetadataEvolutionSupport.updateMetadataTrackingLogAndFailTheStreamIfNeeded$(DeltaSourceMetadataEvolutionSupport.scala:398)
	at org.apache.spark.sql.delta.sources.DeltaSource.updateMetadataTrackingLogAndFailTheStreamIfNeede
```
The error is pretty clear. It states that the schema evolution was indeed accepted but since the schema changes,
you should be aware of it and restart the job.

5. Run [restart_streaming_after_rename.py](restart_streaming_after_rename.py). This time you should see the job
correctly processing the data after rename:

```
-------------------------------------------
Batch: 2
-------------------------------------------
+--------+----------+----+----+
|visit_id|event_time|user|page|
+--------+----------+----+----+
+--------+----------+----+----+

-------------------------------------------
Batch: 3
-------------------------------------------
+---------+----------------------+---------+------------+
|visit_id |event_time            |user     |page        |
+---------+----------------------+---------+------------+
|visit 100|2024-07-02 12:00:00.84|user---10|contact.html|
+---------+----------------------+---------+------------+
```

6. As you can see, the schema tracking feature helps migrating schema more easily. However, there is a glitch.
If your data processing logic operates on the renamed columns, your job will fail.

7. Explain the [processing_restart_streaming_after_rename.py](processing_restart_streaming_after_rename.py) and [processing_start_streaming_and_table_rename.py](processing_start_streaming_and_table_rename.py)
* the jobs are the same as previously executed but the difference is their transformation logic that contains an 
extra mapping task (`.withColumn('user_id_exists', functions.isnotnull('user_id'))`)

8. Run `create_table.py`.
9. Run the `processing_restart_streaming_after_rename.py`. Once again, you should get an error:
```
pyspark.errors.exceptions.captured.StreamingQueryException: [STREAM_FAILED] Query [id = b2a76275-6a42-40a6-8652-d250c4e900ef, runId = f7e3aa2e-a6c3-4fcf-b0b1-8ed93c028ca0] terminated with exception: [DELTA_STREAMING_METADATA_EVOLUTION] The schema, table configuration or protocol of your Delta table has changed during streaming.
The schema or metadata tracking log has been updated.
Please restart the stream to continue processing using the updated metadata.
Updated schema: root
 |-- visit_id: string (nullable = false)
 |-- event_time: timestamp (nullable = false)
 |-- user: string (nullable = false)
 |-- page: string (nullable = false)
.
Updated table configurations: delta.columnMapping.mode:name, delta.columnMapping.maxColumnId:4.
Updated table protocol: 2,5
```

10. Run the `processing_restart_streaming_after_rename.py`. This time, the query fails because of the transformation
method operating on the old name:
```
pyspark.errors.exceptions.captured.AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `user_id` cannot be resolved. Did you mean one of the following? [`user`, `visit_id`, `page`, `event_time`].;
'Project [visit_id#39, event_time#40, user#41, page#42, isnotnull('user_id) AS user_id_exists#47]
+- SubqueryAlias spark_catalog.default.visits
   +- StreamingRelation DataSource(org.apache.spark.sql.SparkSession@751eb752,delta,List(),Some(StructType()),List(),None,Map(schemaTrackingLocation -> /tmp/dedp/ch09/02-schema-consistency/02-schema-versioner-apache-spark-delta-lake/checkpoint-processing/schema_tracking, path -> file:/home/bartosz/workspace/data-engineering-design-patterns-book-private/chapter-09/02-schema-consistency/02-schema-versioner-apache-spark-delta-lake/spark-warehouse/visits),Some(CatalogTable(
```

The Schema tracking feature is not there to automatically migrate the job. It does indeed help by mapping old columns
to the new ones but it stops the query whenever a schema change is detected. It's a warning for you that you might need to 
adapt the processing logic, as we should have done in our last example.