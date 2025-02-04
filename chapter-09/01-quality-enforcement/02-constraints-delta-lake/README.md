# Quality enforcement - constraints with Delta Lake

1. Explain the [table_with_constraints.py](table_with_constraints.py)
* the job creates a new table with only the required field (`NOT NULL`)
* next, it adds extra protection mechanisms with `ADD CONSTRAINT` to ensure the event_time is not in the future
* in the next part, the job verifies the metadata
* and finally, it adds the records; a valid one first and two invalid later
2. Run the `table_with_constraints.py`
The job should show the _checkConstraints_ table feature alongside the _event_time_not_in_the_future_ constraint 
in the properties:
```
+----------------------------+------------------------------------------+
|name                        |tableFeatures                             |
+----------------------------+------------------------------------------+
|spark_catalog.default.visits|[appendOnly, checkConstraints, invariants]|
+----------------------------+------------------------------------------+

+----------------------------------------------+------------------------------------------+
|key                                           |value                                     |
+----------------------------------------------+------------------------------------------+
|delta.constraints.event_time_not_in_the_future|event_time < NOW ( ) + INTERVAL "1 SECOND"|
|delta.minReaderVersion                        |1                                         |
|delta.minWriterVersion                        |3                                         |
+----------------------------------------------+------------------------------------------+
```

As the first insert doesn't break any of the constraints, it should add the record:
```
+--------+----------------------+-------+----------+
|visit_id|event_time            |user_id|page      |
+--------+----------------------+-------+----------+
|visit 1 |2024-07-01 12:00:00.84|user 1 |index.html|
+--------+----------------------+-------+----------+
```

But it shouldn't be the case for two next inserts. The first breaks the nullability constraint and the write
should fail with:

```
org.apache.spark.sql.delta.schema.DeltaInvariantViolationException: [DELTA_NOT_NULL_CONSTRAINT_VIOLATED] NOT NULL constraint violated for column: visit_id.
```

The second insert breaks the event-time constraint, so it should fail with a different message:
```
org.apache.spark.sql.delta.schema.DeltaInvariantViolationException: [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint event_time_not_in_the_future (event_time < (NOW() + INTERVAL '01' SECOND)) violated by row with values:
```

There is also the final test to see whether a batch of records with failed rows will partially succeed. 
Thanks to the ACID guarantee, it won't and as two last outputs, you should see:
```
org.apache.spark.sql.delta.schema.DeltaInvariantViolationException: [DELTA_NOT_NULL_CONSTRAINT_VIOLATED] NOT NULL constraint violated for column: visit_id.

...

+--------+----------------------+-------+----------+
|visit_id|event_time            |user_id|page      |
+--------+----------------------+-------+----------+
|visit 1 |2024-07-01 12:00:00.84|user 1 |index.html|
+--------+----------------------+-------+----------+
```
3. Explain the [table_without_constraints.py](table_without_constraints.py)
* the code is the same except the table doesn't have the constraints
4. Run `table_without_constraints.py`
As a result, you will get a dataset with a lot of data quality issues that will require to be solved
on the consumers' side:

```
+----------------------------+------------------------+
|name                        |tableFeatures           |
+----------------------------+------------------------+
|spark_catalog.default.visits|[appendOnly, invariants]|
+----------------------------+------------------------+

+----------------------+-----+
|key                   |value|
+----------------------+-----+
|delta.minReaderVersion|1    |
|delta.minWriterVersion|2    |
+----------------------+-----+

+--------+----------------------+-------+----------+
|visit_id|event_time            |user_id|page      |
+--------+----------------------+-------+----------+
|visit 1 |2024-07-01 12:00:00.84|user 1 |index.html|
+--------+----------------------+-------+----------+

+-----------------------+----------------------+----------------------+----------+
|visit_id               |event_time            |user_id               |page      |
+-----------------------+----------------------+----------------------+----------+
|visit 2 from the future|2036-07-01 12:00:00.84|user 2 from the future|index.html|
|visit 1                |2024-07-01 12:00:00.84|user 1                |index.html|
|NULL                   |2023-07-01 12:00:00.84|user 2                |index.html|
+-----------------------+----------------------+----------------------+----------+

+-----------------------+----------------------+----------------------+----------+
|visit_id               |event_time            |user_id               |page      |
+-----------------------+----------------------+----------------------+----------+
|visit 2 from the future|2036-07-01 12:00:00.84|user 2 from the future|index.html|
|visit 1                |2024-07-01 12:00:00.84|user 1                |index.html|
|visit 1                |2024-07-01 12:00:00.84|user 1                |index.html|
|NULL                   |2023-07-01 12:00:00.84|user 2                |index.html|
|NULL                   |2023-07-01 12:00:00.84|user 2                |index.html|
+-----------------------+----------------------+----------------------+----------+
```
