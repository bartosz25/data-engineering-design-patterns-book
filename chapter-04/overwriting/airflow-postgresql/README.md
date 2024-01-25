# Overwriting - fast metadata cleaner
1. Prepare the directory with the data:
```
mkdir -p /tmp/dedp/ch04/overwriting/input
```
2. Start the Apache Airflow instance:
```
./start.sh
```
3. Open the Apache Airflow UI and connect: http://localhost:8080 (dedp/dedp)
4. Start the PostgreSQL database:
```
cd docker
docker-compose down --volumes; docker-compose up
```

5. Explain the [idempotency_metadata_overwrite.py](dags%2Fidempotency_metadata_overwrite.py)
* the DAG follows the Fast metadata cleaner pattern 
  * first, it detects whether it should create a new _idempotency_ table (weekly granularity)
  * if it's the case, the pipeline follows the branch for table creation and view update; all
    _idempotency tables_ are exposed from a single place that also needs to be refreshed with each new table
  * otherwise, the pipeline follows a _dummy marker task_
  * next, it loads the data to the weekly table
⚠️ The demo is there for the Fast metadata cleaner pattern and the pipeline doesn't have any data quality controls
   to keep it simple for understanding. You should add these extra controls for production workloads, though. They're 
   also present in the book's Data quality chapter.
6. Access the UI http://localhost:8080/home (dedp/dedp) and start the pipeline. It should run 4 times.
7. Check the table's content after the last planed run:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
dedp=# SELECT COUNT(*) FROM (SELECT COUNT(*) FROM dedp.visits GROUP BY visit_id, event_time HAVING COUNT(*) = 1) AS t;
 count 
-------
   200
(1 row)

dedp=# SELECT COUNT(*) FROM (SELECT COUNT(*) FROM dedp.visits GROUP BY visit_id, event_time HAVING COUNT(*) > 1) AS t;
 count 
-------
     0
(1 row)

dedp=# SELECT COUNT(*) FROM (SELECT COUNT(*) FROM dedp.visits_json_copy_week_45_2023 GROUP BY visit_id, event_time HAVING COUNT(*) = 1) AS t;
 count 
-------
   200
(1 row)

dedp=# SELECT COUNT(*) FROM (SELECT COUNT(*) FROM dedp.visits_json_copy_week_45_2023 GROUP BY visit_id, event_time HAVING COUNT(*) > 1) AS t;
 count 
-------
     0
(1 row)
```
You should see only unique values in the view and weekly table.

8. Clean the DAG's state to relaunch the pipelines.
![clear_tasks.png](assets%2Fclear_tasks.png)
9. Validate the database again. You shouldn't see any duplicates here too.
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
dedp=# SELECT COUNT(*) FROM (SELECT COUNT(*) FROM dedp.visits GROUP BY visit_id, event_time HAVING COUNT(*) = 1) AS t;
 count 
-------
   200
(1 row)

dedp=# SELECT COUNT(*) FROM (SELECT COUNT(*) FROM dedp.visits GROUP BY visit_id, event_time HAVING COUNT(*) > 1) AS t;
 count 
-------
     0
(1 row)

dedp=# SELECT COUNT(*) FROM (SELECT COUNT(*) FROM dedp.visits_json_copy_week_45_2023 GROUP BY visit_id, event_time HAVING COUNT(*) = 1) AS t;
 count 
-------
   200
(1 row)

dedp=# SELECT COUNT(*) FROM (SELECT COUNT(*) FROM dedp.visits_json_copy_week_45_2023 GROUP BY visit_id, event_time HAVING COUNT(*) > 1) AS t;
 count 
-------
     0
(1 row)
```