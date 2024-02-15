# Decorator - metadata table
1. Prepare the directory with the data:
```
mkdir -p /tmp/dedp/ch05/02-decorator/02-metadata-airflow-postgresql/input
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
* it's the same DAG as the one from Chapter 4 and the single change is a new _visits_context_ table referenced
  in [load_visits_to_weekly_table.sql](sql%2Fload_visits_to_weekly_table.sql)
  * the code first loads the input records to a temporary table, later inserts the metadata entry,
    and finally loads all content from the temporary table to the final one
6. Access the UI http://localhost:8080/home (dedp/dedp) and start the pipeline. It should run 4 times.
7. Check the table's content after the last planed run:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
dedp=# SELECT * FROM dedp.visits_context;
  execution_date_time   |         loading_time          | code_version | loading_attempt 
------------------------+-------------------------------+--------------+-----------------
 2023-11-06 00:00:00+00 | 2024-02-15 15:13:43.768665+00 | v1.3.4       |               2
 2023-11-07 00:00:00+00 | 2024-02-15 15:15:03.535997+00 | v1.3.4       |               1
 2023-11-08 00:00:00+00 | 2024-02-15 15:15:13.302892+00 | v1.3.4       |               1
 2023-11-09 00:00:00+00 | 2024-02-15 15:15:23.789646+00 | v1.3.4       |               1
(4 rows)

```
You should see one record for each execution. Additionally, you should be able to join data rows 
with the metadata table and get the data loading information, just like in the snippet below:

```
dedp=# SELECT visit_id, c.* FROM dedp.visits v JOIN dedp.visits_context c ON c.execution_date_time = v.visits_context_execution_date_time ORDER BY RANDOM() LIMIT 10;
               visit_id               |  execution_date_time   |         loading_time          | code_version | loading_attempt 
--------------------------------------+------------------------+-------------------------------+--------------+-----------------
 140282463124352_43                   | 2023-11-09 00:00:00+00 | 2024-02-15 15:15:23.789646+00 | v1.3.4       |               1
 140282463124352_28                   | 2023-11-09 00:00:00+00 | 2024-02-15 15:15:23.789646+00 | v1.3.4       |               1
 140282463124352_18                   | 2023-11-09 00:00:00+00 | 2024-02-15 15:15:23.789646+00 | v1.3.4       |               1
 140282463124352_5                    | 2023-11-08 00:00:00+00 | 2024-02-15 15:15:13.302892+00 | v1.3.4       |               1
 140282463124352_45                   | 2023-11-09 00:00:00+00 | 2024-02-15 15:15:23.789646+00 | v1.3.4       |               1
 140282463124352_46                   | 2023-11-06 00:00:00+00 | 2024-02-15 15:13:43.768665+00 | v1.3.4       |               2
 140282463124352_35                   | 2023-11-06 00:00:00+00 | 2024-02-15 15:13:43.768665+00 | v1.3.4       |               2
 140282463124352_2                    | 2023-11-06 00:00:00+00 | 2024-02-15 15:13:43.768665+00 | v1.3.4       |               2
 140282463124352_49                   | 2023-11-06 00:00:00+00 | 2024-02-15 15:13:43.768665+00 | v1.3.4       |               2
 140282463124352_41                   | 2023-11-09 00:00:00+00 | 2024-02-15 15:15:23.789646+00 | v1.3.4       |               1
(10 rows)
```