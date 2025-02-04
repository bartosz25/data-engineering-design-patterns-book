# Schema enforcement - schema evolution with Delta Lake

1. Explain the [table_with_schema_enforcement.py](table_with_schema_enforcement.py)
* the code creates a new table composed of three fields: visit_id: string (nullable = true), 
page: string (nullable = true), event_time: long (nullable = true)
* next, the job inserts a record that comply to the schema, and another one that breaks the schema by 
adding an extra field
2. Run the `table_with_schema_enforcement.py`. You should see an error saying that it's not possible to 
add new fields dynamically:
```
pyspark.errors.exceptions.captured.AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: b8ece77b-71c4-49a0-9c02-13a19ddd01d0).
To enable schema migration using DataFrameWriter or DataStreamWriter, please set:
'.option("mergeSchema", "true")'.
For other operations, set the session configuration
spark.databricks.delta.schema.autoMerge.enabled to "true". See the documentation
specific to the operation for details.

Table schema:
root
-- visit_id: string (nullable = true)
-- page: string (nullable = true)
-- event_time: long (nullable = true)


Data schema:
root
-- visit_id: string (nullable = true)
-- page: string (nullable = true)
-- event_time: long (nullable = true)
-- ad_id: string (nullable = true)
```

3. Explain the [table_with_schema_enforcement_types.py](table_with_schema_enforcement_types.py)
* it's similar example to the previous one except that the modification is about a different type; as you can notice
the timestamp represented as an epoch time, it's now set as a string

4. Run the `table_with_schema_enforcement_types.py`. You should see an exception like:
```
Caused by: org.apache.spark.SparkNumberFormatException: [CAST_INVALID_INPUT] The value '2024-05-05T09:00:00' of the type "STRING" cannot be cast to "BIGINT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error.
	at org.apache.spark.sql.errors.QueryExecutionErrors$.invalidInputInCastToNumberError(QueryExecutionErrors.scala:145)
	at org.apache.spark.sql.catalyst.util.UTF8StringUtils$.withException(UTF8StringUtils.scala:51)
	at org.apache.spark.sql.catalyst.util.UTF8StringUtils$.toLongExact(UTF8StringUtils.scala:31)
	at org.apache.spark.sql.catalyst.util.UTF8StringUtils.toLongExact(UTF8StringUtils.scala)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:385)

```

5. Explain the [table_with_schema_enforcement_casted_types.py](table_with_schema_enforcement_casted_types.py)
* the example is similar to the previous one; the single difference now is the field that changed the type
  * the page is set as an integer but the framework could cast it to a higher-type which is the string
6. Run the `table_with_schema_enforcement_casted_types.py`. The evolution should work and insert a new page. 
The last page will still be a string, though:

```
+--------+------------+----------+
|visit_id|page        |event_time|
+--------+------------+----------+
|event2  |contact.html|456       |
|event4  |index.html  |91011     |
|event1  |index.html  |123       |
|event3  |about.html  |789       |
|event4  |12          |100       |
+--------+------------+----------+

root
 |-- visit_id: string (nullable = true)
 |-- page: string (nullable = true)
 |-- event_time: long (nullable = true)
```