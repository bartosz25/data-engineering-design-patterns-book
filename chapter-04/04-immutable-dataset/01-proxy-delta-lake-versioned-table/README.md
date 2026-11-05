# Proxy design pattern - versioned Delta Lake table

1. Explain [versioned_table.py](versioned_table.py)
* the job fully overwrites the table in each run
  * under-the-hood it doesn't remove the existing data; it only adds the new rows on top of the existing ones
  * consequently, any reader always retrieves the most recent version
    * unless a reader uses the `VERSION AS OF` operatio to query one of past table versions

2. Run versioned_table.py:
```shell
python versioned_table.py
```

Despite the 3 writes, the first SELECT should get the most recently written records only...
```
Most recent version
+-------+-------------+
|id     |value        |
+-------+-------------+
|third_1|third_value_1|
|third_2|third_value_2|
+-------+-------------+
```

...while the second SELECT - the one that uses SYSTEM VERSION AS OF - should get the data after the 2nd write:
```
Historical version (2nd generated DataFrame)
+--------+--------------+
|id      |value         |
+--------+--------------+
|second_1|second_value_1|
|second_2|second_value_2|
+--------+--------------+
```