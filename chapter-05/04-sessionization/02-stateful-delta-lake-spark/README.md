# Sessionization - Apache Spark and Delta Lake with stateful processing
1. Setup the directories:
```commandline
rm -rf /tmp/dedp/ch05/04-sessionization/02-stateful-delta-lake-spark
mkdir -p /tmp/dedp/ch05/04-sessionization/02-stateful-delta-lake-spark
```

2. Generate the first set of rows:
```
python generate_new_rows.py
```

3. Validate the data is in the table by running `read_tables.py`. You should see:
```
+-------+--------------------------+-----------+
|user_id|visit_time                |page       |
+-------+--------------------------+-----------+
|1      |2026-02-15 17:05:34.432306|page_1.html|
|1      |2026-02-15 17:05:34.432317|page_2.html|
|2      |2026-02-15 17:05:34.432319|page_3.html|
|3      |2026-02-15 17:05:34.43232 |page_4.html|
+-------+--------------------------+-----------+
```
4. Explain the [incremental_sessionizer.py](incremental_sessionizer.py)
* the job uses `.trigger(availableNow=True)` to incrementally load new visits and remember
the progress in the checkpoint files
* the job also leverage stateful processing and the state store to persist pending sessions
5. Run `incremental_sessionizer.py`. 
6. Run `read_session_tables.py`. The sessions table should be empty due to the _append_ semantics that outputs rows after the watermark:
```
+----------+--------+-------------+------------------------+-------+
|start_time|end_time|visited_pages|duration_in_milliseconds|user_id|
+----------+--------+-------------+------------------------+-------+
+----------+--------+-------------+------------------------+-------+
```
7. Generate new rows `python generate_new_rows.py`
8. Run `incremental_sessionizer.py`. 
9. Run `read_session_tables.py`. The table should still be empty as it still hasn't reached the watermark:
```
+----------+--------+-------------+------------------------+-------+
|start_time|end_time|visited_pages|duration_in_milliseconds|user_id|
+----------+--------+-------------+------------------------+-------+
+----------+--------+-------------+------------------------+-------+
```
10. Run `generate_closing_rows.py`
11. Run `incremental_sessionizer.py`. 
12. Run `read_session_tables.py`. You should see some of the pending sessions closed:
```
+-------------------+-------------------+------------------------------------------------------------------------------------------------------------------------+------------------------+-------+
|start_time         |end_time           |visited_pages                                                                                                           |duration_in_milliseconds|user_id|
+-------------------+-------------------+------------------------------------------------------------------------------------------------------------------------+------------------------+-------+
|2026-02-15 18:05:34|2026-02-15 18:06:47|[{page_1.html, 1771175134000}, {page_2.html, 1771175134000}, {page_1.html, 1771175207000}, {page_2.html, 1771175207000}]|73000                   |1      |
|2026-02-15 18:05:34|2026-02-15 18:06:47|[{page_3.html, 1771175134000}, {page_3.html, 1771175207000}]                                                            |73000                   |2      |
|2026-02-15 18:05:34|2026-02-15 18:06:47|[{page_4.html, 1771175134000}, {page_4.html, 1771175207000}]                                                            |73000                   |3      |
|2026-02-15 19:09:18|2026-02-15 19:09:18|[{page_1.html, 1771178958000}]                                                                                          |0                       |4      |
+-------------------+-------------------+------------------------------------------------------------------------------------------------------------------------+------------------------+-------+
```
