# Sessionization - Apache Spark and Delta Lake
**TODO remove is_completed**
1. Setup the directories:
```commandline
rm -rf /tmp/dedp/ch05/04-sessionization/01-incremental-delta-lake-spark
mkdir -p /tmp/dedp/ch05/04-sessionization/01-incremental-delta-lake-spark
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
|1      |2026-02-15 13:23:19.045843|page_1.html|
|1      |2026-02-15 13:23:19.045856|page_2.html|
|2      |2026-02-15 13:23:19.045859|page_3.html|
|3      |2026-02-15 13:23:19.04586 |page_4.html|
+-------+--------------------------+-----------+
```
4. Explain the [incremental_sessionizer.py](incremental_sessionizer.py)
* the job uses `.trigger(availableNow=True)` to incrementally load new visits and remember
the progress in the checkpoint files
* inside the `foreachBatch` function, the job combines pending sessions with the new rows
  * it writes completed sessions to a `SESSIONS_DIR` table
  * additionally, all pending sessions are written with their batch number to `PENDING_SESSIONS_DIR` table
5. Run `incremental_sessionizer.py`. 
6. Run `read_session_tables.py`. You should see:
```
PENDING SESSIONS
+-------+--------------------------+------------+
|user_id|concatenated_pages        |batch_number|
+-------+--------------------------+------------+
|1      |page_1.html -> page_2.html|0           |
|2      |page_3.html               |0           |
|3      |page_4.html               |0           |
+-------+--------------------------+------------+

SESSIONS
+-------+------------------+
|user_id|concatenated_pages|
+-------+------------------+
+-------+------------------+
```
7. Generate new rows `python generate_new_rows.py`
8. Run `incremental_sessionizer.py`. 
9. Run `read_session_tables.py`. You should see:
```
PENDING SESSIONS
+-------+--------------------------------------------------------+------------+
|user_id|concatenated_pages                                      |batch_number|
+-------+--------------------------------------------------------+------------+
|1      |page_1.html -> page_2.html                              |0           |
|3      |page_4.html                                             |0           |
|2      |page_3.html                                             |0           |
|3      |page_4.html -> page_4.html                              |1           |
|1      |page_1.html -> page_2.html -> page_1.html -> page_2.html|1           |
|2      |page_3.html -> page_3.html                              |1           |
+-------+--------------------------------------------------------+------------+

SESSIONS
+-------+------------------+
|user_id|concatenated_pages|
+-------+------------------+
+-------+------------------+
```
10. Run `generate_closing_rows.py`
11. Run `incremental_sessionizer.py`. 
12. Run `read_session_tables.py`. You should see some of the pending sessions closed:
```
PENDING SESSIONS
+-------+--------------------------------------------------------+------------+
|user_id|concatenated_pages                                      |batch_number|
+-------+--------------------------------------------------------+------------+
|2      |page_3.html                                             |0           |
|1      |page_1.html -> page_2.html                              |0           |
|3      |page_4.html                                             |0           |
|3      |page_4.html -> page_4.html                              |1           |
|1      |page_1.html -> page_2.html -> page_1.html -> page_2.html|1           |
|2      |page_3.html -> page_3.html                              |1           |
|4      |page_2.html                                             |2           |
|3      |page_1.html -> page_4.html -> page_4.html               |2           |
+-------+--------------------------------------------------------+------------+

SESSIONS
+-------+--------------------------------------------------------+
|user_id|concatenated_pages                                      |
+-------+--------------------------------------------------------+
|1      |page_1.html -> page_2.html -> page_1.html -> page_2.html|
|2      |page_3.html -> page_3.html                              |
+-------+--------------------------------------------------------+
```
> [!NOTE]  
> Reprocessing boundaries are tied to the number of retained checkpoints.