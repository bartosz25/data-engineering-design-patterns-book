# Late data detector - Apache Spark Structured Streaming and GC watermark
1. Start Docker containers:
```
cd docker
docker-compose down --volumes; docker-compose up

```
2. Explain the [visits_per_10_minutes.py](visits_per_10_minutes.py)
* the job first prepares the data to be processed by extracting all required columns
* next, it assigns the watermark to the input data through the `withWatermark` function
* after, the job defines the stateful logic that will be impacted by the watermark
  * watermark will control two aspects:
    * how long each window is kept in the state store to include the most complete set of observations
    * as a result, it also impacts what late records can integrate the processing part; only the ones for which
      windows exist or can be created in the state store 
3. Run the `visits_per_10_minutes.py`
You should expect the following output which is self-explanatory for the watermark and windows output:
```
-------------------------------------------
Batch: 0
-------------------------------------------
+------+-----+------------+
|window|count|current_time|
+------+-----+------------+
+------+-----+------------+

{'watermark': '1970-01-01T00:00:00.000Z'}
Producing {"visit_id": 1, "event_time": "2023-06-30T03:15:00Z", "page": "page1"}
-------------------------------------------
Batch: 1
-------------------------------------------
+------+-----+------------+
|window|count|current_time|
+------+-----+------------+
+------+-----+------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------+-----+------------+
|window|count|current_time|
+------+-----+------------+
+------+-----+------------+

{'watermark': '2023-06-30T02:15:00.000Z'}
Producing {"visit_id": 1, "event_time": "2023-06-30T03:00:00Z", "page": "page2"}
-------------------------------------------
Batch: 3
-------------------------------------------
+------+-----+------------+
|window|count|current_time|
+------+-----+------------+
+------+-----+------------+

{'avg': '2023-06-30T03:00:00.000Z', 'max': '2023-06-30T03:00:00.000Z', 'min': '2023-06-30T03:00:00.000Z', 'watermark': '2023-06-30T02:15:00.000Z'}
Producing {"visit_id": 1, "event_time": "2023-06-30T01:50:00Z", "page": "page3"}
-------------------------------------------
Batch: 4
-------------------------------------------
+------+-----+------------+
|window|count|current_time|
+------+-----+------------+
+------+-----+------------+

{'avg': '2023-06-30T01:50:00.000Z', 'max': '2023-06-30T01:50:00.000Z', 'min': '2023-06-30T01:50:00.000Z', 'watermark': '2023-06-30T02:15:00.000Z'}
Producing {"visit_id": 1, "event_time": "2023-06-30T03:11:00Z", "page": "page3"}
-------------------------------------------
Batch: 5
-------------------------------------------
+------+-----+------------+
|window|count|current_time|
+------+-----+------------+
+------+-----+------------+

{'avg': '2023-06-30T03:11:00.000Z', 'max': '2023-06-30T03:11:00.000Z', 'min': '2023-06-30T03:11:00.000Z', 'watermark': '2023-06-30T02:15:00.000Z'}
Producing {"visit_id": 1, "event_time": "2023-06-30T04:31:00Z", "page": "page3"}
-------------------------------------------
Batch: 6
-------------------------------------------
+------+-----+------------+
|window|count|current_time|
+------+-----+------------+
+------+-----+------------+

-------------------------------------------
Batch: 7
-------------------------------------------
+------------------------------------------+-----+-----------------------+
|window                                    |count|current_time           |
+------------------------------------------+-----+-----------------------+
|{2023-06-30 03:00:00, 2023-06-30 03:10:00}|1    |2024-01-16 14:01:34.378|
|{2023-06-30 03:10:00, 2023-06-30 03:20:00}|2    |2024-01-16 14:01:34.378|
+------------------------------------------+-----+-----------------------+

{'watermark': '2023-06-30T03:31:00.000Z'}
```