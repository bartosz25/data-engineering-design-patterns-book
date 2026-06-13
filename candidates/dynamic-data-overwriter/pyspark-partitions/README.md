# Dynamic Data Overwriter - Apache Spark and partitioned Delta Lake

1. Explain the [dynamic_overwrite_job.py](dynamic_overwrite_job.py)
* the job starts by creating a table partitioned by the _website_id_ column
* next, the job performs an insert operation with the dynamic partition overwrite and DataFrame overwrite mode; 
  * both modes lead to:
    * overwriting partitions present in the table and in the written `DataFrame`
    * adding partitions missing in the table but present in the written `DataFrame`
2. Run `dynamic_overwrite_job.py`
You should see two tables: 
```
+----------+--------+------------+----------+
|website_id|visit_id|page        |event_time|
+----------+--------+------------+----------+
|1         |event1  |index.html  |123       |
|1         |event2  |contact.html|456       |
|2         |event3  |about.html  |789       |
+----------+--------+------------+----------+
```
After the first write, you should have two partitions (1 and 2). The partition 2 should be next
fully replaced with the data present in the new `DataFrame`. Besides, a new partition should be created
for the website 3:
```
+----------+--------+---------------+----------+
|website_id|visit_id|page           |event_time|
+----------+--------+---------------+----------+
|1         |event1  |index.html     |123       |
|1         |event2  |contact.html   |456       |
|2         |event3  |about_page.html|789       |
|2         |event4  |contact.html   |1011      |
|3         |event5  |index.html     |1213      |
+----------+--------+---------------+----------+
```