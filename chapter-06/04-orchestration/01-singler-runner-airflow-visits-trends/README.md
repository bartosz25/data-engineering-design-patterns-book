# Orchestration - single runner

1. Prepare the dataset:
```
cd docker
mkdir -p /tmp/dedp/ch06/04-orchestration/01-single-runner-airflow-visits-trends/input
docker-compose down --volumes; docker-compose up
```

2. Start the Apache Airflow instance:
```
cd ../
./start.sh
```
3. Open the Apache Airflow UI and connect: http://localhost:8080 (dedp/dedp)
4. Explain the [visits_trends_generator.py](dags%2Fvisits_trends_generator.py)
* the DAG is configured as _single runner_ because of the following properties:
  * `'depend_on_past': True,` - means that one task cannot start as long as its predecessor (same task from the previous
  execution) didn't succeed
  * `max_active_runs=1,` - means there is only one pipeline running at a given time
* the reason behind this requirement is related to the business logic, and more specifically, to how the 
  code computes trends: 
```sql
INSERT INTO dedp.visits_trend (current_execution_time_id, visits_trend)
    SELECT  '{{ ds }}',
        (SELECT COUNT(*) AS current_count FROM dedp.visits_raw WHERE execution_time_id = '{{ ds }}') -
        (SELECT COUNT(*) AS previous_count FROM dedp.visits_raw WHERE execution_time_id = '{{ prev_ds }}') AS
        visits_trend
;
```
As you can see, the current run (`{{ ds }}`) can't run if the previous one (`{{ prev_ds }}`) hasn't completed.
Otherwise, it'll generate fake and only growing trends.

5. Run the `visits_trends_generator` and wait until it completes all executions.
6. Check the results in the database:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
dedp=# SELECT * FROM dedp.visits_trend;
 current_execution_time_id | visits_trend 
---------------------------+--------------
 2024-02-01                |           10
 2024-02-02                |            0
 2024-02-03                |            0
 2024-02-04                |            0
 2024-02-05                |            0
(5 rows)
``` 
As we use the same dataset for all executions, the trends are +10 (no prior run, we might ignore it but let's keep
it simple for the demo), 0, 0, 0, 0, as each run ingested 10 rows. Let's confirm it here:

```
dedp=# SELECT COUNT(*), execution_time_id FROM dedp.visits_raw GROUP BY execution_time_id;
 count | execution_time_id 
-------+-------------------
    10 | 2024-02-01
    10 | 2024-02-05
    10 | 2024-02-03
    10 | 2024-02-04
    10 | 2024-02-02
(5 rows)
```