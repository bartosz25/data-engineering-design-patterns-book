1. Create the dataset:
```
cd ../dataset
mkdir -p /tmp/dedp/ch02/incremental-load/change-data-capture/
docker-compose down --volumes; docker-compose up
```
2. Open [events_table_streaming_reader.py](events_table_streaming_reader.py)
* the code is divided into 2 parts:
  * `load_data_to_the_events_table` - it's the data generator; it generates partitioned dataset 
     and interacts with a higher level `Lock` 
  * the next part of the code waits for the first dataset to be generated and just after starts 
    processing the partitions in a streaming way
    * 
3. Run the `events_table_streaming_reader.py`
* you should see the generated visits streamed from the tables concurrently to the loading process, e.g.
```

Loading partition date=2023-11-05
-------------------------------------------
Batch: 3
-------------------------------------------
+------------------+-------------------+--------------------+-----------+------------+---------------+--------------------+
|          visit_id|         event_time|             user_id|       page|_change_type|_commit_version|   _commit_timestamp|
+------------------+-------------------+--------------------+-----------+------------+---------------+--------------------+
| 139627729791872_0|2023-11-01 01:10:00|139627729791872_4...|    page_10|      insert|              4|2023-12-25 13:41:...|
| 139627729791872_1|2023-11-01 01:13:00|139627729791872_5...|       home|      insert|              4|2023-12-25 13:41:...|
| 139627729791872_2|2023-11-01 01:08:00|139627729791872_e...|      about|      insert|              4|2023-12-25 13:41:...|
| 139627729791872_3|2023-11-01 01:17:00|139627729791872_5...| categories|      insert|              4|2023-12-25 13:41:...|
...
```