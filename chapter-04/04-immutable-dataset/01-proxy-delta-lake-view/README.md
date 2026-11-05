# Proxy design pattern - partitioned Delta Lake table

1. Explain [view_partitioned_table.py](view_partitioned_table.py)
* the code creates a table partitioned by the current timestamp
  * to avoid overwriting the whole table with a new partition, the `SparkSession` explicitly uses
    the `.config("spark.sql.sources.partitionOverwriteMode", "dynamic")`

2. Run the view_partitioned_table.py:
```shell
python view_partitioned_table.py
```

The code creates a view that always returns the most recent version of the data:
```
Most recent version after the 1st run
+-------+-------------+--------------------------+
|id     |value        |partition_timestamp       |
+-------+-------------+--------------------------+
|first_1|first_value_1|2026-11-04 17:45:46.154832|
|first_2|first_value_2|2026-11-04 17:45:46.154832|
+-------+-------------+--------------------------+

Most recent version after the 2nd run
+--------+--------------+--------------------------+
|id      |value         |partition_timestamp       |
+--------+--------------+--------------------------+
|second_2|second_value_2|2026-11-04 17:45:51.073862|
|second_1|second_value_1|2026-11-04 17:45:51.073862|
+--------+--------------+--------------------------+

Most recent version after the 3rd run
+-------+-------------+--------------------------+
|id     |value        |partition_timestamp       |
+-------+-------------+--------------------------+
|third_1|third_value_1|2026-11-04 17:45:52.450922|
|third_2|third_value_2|2026-11-04 17:45:52.450922|
+-------+-------------+--------------------------+
```

In the end it contains an example on what happens when you query this table without the view. As the partitioning is dynamic,
it returns all the values; the partition overwrite only targets the partition present in the written `DataFrame`, the others
are left untouched:
```
Here is what happens when you query the table directly:
+--------+--------------+--------------------------+
|id      |value         |partition_timestamp       |
+--------+--------------+--------------------------+
|second_2|second_value_2|2026-11-04 17:45:51.073862|
|second_1|second_value_1|2026-11-04 17:45:51.073862|
|first_1 |first_value_1 |2026-11-04 17:45:46.154832|
|first_2 |first_value_2 |2026-11-04 17:45:46.154832|
|third_1 |third_value_1 |2026-11-04 17:45:52.450922|
|third_2 |third_value_2 |2026-11-04 17:45:52.450922|
+--------+--------------+--------------------------+
```