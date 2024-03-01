# Sessionization - windows, Apache Airflow, and PostgreSQL
1. Prepare the dataset:
```
cd docker
mkdir -p /tmp/dedp/ch05/04-sessionization/01-incremental-airflow-windows-postgresql/input
docker-compose down --volumes; docker-compose up
```

Remove 3 last lines in each file so that the sessions can expire:
```
DATA_FILE=/tmp/dedp/ch05/04-sessionization/01-incremental-airflow-windows-postgresql/input/date\=20240203/dataset.csv
head -n -3 $DATA_FILE > /tmp/dataset.csv
mv /tmp/dataset.csv $DATA_FILE
DATA_FILE=/tmp/dedp/ch05/04-sessionization/01-incremental-airflow-windows-postgresql/input/date\=20240204/dataset.csv
head -n -3 $DATA_FILE > /tmp/dataset.csv
mv /tmp/dataset.csv $DATA_FILE
DATA_FILE=/tmp/dedp/ch05/04-sessionization/01-incremental-airflow-windows-postgresql/input/date\=20240205/dataset.csv
head -n -3 $DATA_FILE > /tmp/dataset.csv
mv /tmp/dataset.csv $DATA_FILE
```
2. Start the Apache Airflow instance:
```
cd ../
./start.sh
```
3. Open the Apache Airflow UI and connect: http://localhost:8080 (dedp/dedp)
4. Explain the [sessions_generator.py](dags%2Fsessions_generator.py)
* the DAG starts with two tasks that clean previously generated entries by the same execution
  * ⚠️ the cleaning applies to `execution_time_id >= '{{ ds }}'` because the sessions, albeit incremental,
       are dependent on each other, i.e. the generation for the day _d_ impacts the one for _d+1_, the 
       _d+1_ the generation for _d+2_ and so for
* later, the DAG generates the sessions
  * the task executes a SQL query with the following steps:
    * loading the input dataset to a temporary table; it could be avoided if the dataset was already present in the table
    * _classifying_ the sessions by running a _FULL OUTER JOIN_ between the loaded visits and pending sessions
      written in the previous execution (`WHERE execution_time_id = '{{ prev_ds }}'`)
      * the query pre-processes new visits by applying a `WINDOW visits_window AS (PARTITION BY visit_id, user_id ORDER BY event_time)`
        function
        * Note: we don't care about the ordering of the `pages` column, that's why the window is applied only 
                on the visits and not on the combined visits and pending session
    * in the end, two `INSERT` operations happen:
      * the first one writes the pending visits supposed to expire in the current execution to the final _sessions_ table
      * the second one writes the remaining visits to the _pending_sessions_ table with the expiration time set to 
        `ELSE '{{ macros.ds_add(ds, 2) }}'`; if they don't get any new data back then, they'll be considered as 
        completed
  * alternatively, you could split this into two different tasks, where the first loads and prepares new sessions to the pending
    table, and the second task that generates final sessions
5. Run the `sessions_generator` and wait until it completes all executions.
6. Check the results in the database:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
SELECT session_id, execution_time_id, expiration_batch_id, pages FROM dedp.pending_sessions ORDER BY session_id DESC, execution_time_id LIMIT 20;

                                        session_id                                         | execution_time_id | expiration_batch_id |                      pages                      
-------------------------------------------------------------------------------------------+-------------------+---------------------+-------------------------------------------------
 140053526121344_9                   -140053526121344_288e5cc6-a22f-4ae5-88ad-a7f19a8599b9 | 2024-02-01        | 2024-02-03          | {category_15}
 140053526121344_9                   -140053526121344_288e5cc6-a22f-4ae5-88ad-a7f19a8599b9 | 2024-02-02        | 2024-02-04          | {category_15,main}
 140053526121344_9                   -140053526121344_288e5cc6-a22f-4ae5-88ad-a7f19a8599b9 | 2024-02-03        | 2024-02-04          | {category_15,main}
 140053526121344_8                   -140053526121344_0f8220bf-1c68-4592-bf76-4a69cda55ce8 | 2024-02-01        | 2024-02-03          | {main}
 140053526121344_8                   -140053526121344_0f8220bf-1c68-4592-bf76-4a69cda55ce8 | 2024-02-02        | 2024-02-04          | {main,contact}
 140053526121344_8                   -140053526121344_0f8220bf-1c68-4592-bf76-4a69cda55ce8 | 2024-02-03        | 2024-02-04          | {main,contact}
 140053526121344_7                   -140053526121344_7bb924d2-80b3-426c-a877-d9f1e2029557 | 2024-02-01        | 2024-02-03          | {home}
 140053526121344_7                   -140053526121344_7bb924d2-80b3-426c-a877-d9f1e2029557 | 2024-02-02        | 2024-02-04          | {home,about}
 140053526121344_7                   -140053526121344_7bb924d2-80b3-426c-a877-d9f1e2029557 | 2024-02-03        | 2024-02-04          | {home,about}
 140053526121344_6                   -140053526121344_b1ed1c6c-d2a7-4e42-b337-d0686d791726 | 2024-02-01        | 2024-02-03          | {contact}
 140053526121344_6                   -140053526121344_b1ed1c6c-d2a7-4e42-b337-d0686d791726 | 2024-02-02        | 2024-02-04          | {contact,page_12}
 140053526121344_6                   -140053526121344_b1ed1c6c-d2a7-4e42-b337-d0686d791726 | 2024-02-03        | 2024-02-05          | {contact,page_12,page_10}
 140053526121344_6                   -140053526121344_b1ed1c6c-d2a7-4e42-b337-d0686d791726 | 2024-02-04        | 2024-02-06          | {contact,page_12,page_10,category_7}
 140053526121344_6                   -140053526121344_b1ed1c6c-d2a7-4e42-b337-d0686d791726 | 2024-02-05        | 2024-02-07          | {contact,page_12,page_10,category_7,category_2}
 140053526121344_5                   -140053526121344_49607b37-b10a-4c8b-9e42-e2a601b1c243 | 2024-02-01        | 2024-02-03          | {about}
 140053526121344_5                   -140053526121344_49607b37-b10a-4c8b-9e42-e2a601b1c243 | 2024-02-02        | 2024-02-04          | {about,categories}
 140053526121344_5                   -140053526121344_49607b37-b10a-4c8b-9e42-e2a601b1c243 | 2024-02-03        | 2024-02-05          | {about,categories,index}
 140053526121344_5                   -140053526121344_49607b37-b10a-4c8b-9e42-e2a601b1c243 | 2024-02-04        | 2024-02-06          | {about,categories,index,index}
 140053526121344_5                   -140053526121344_49607b37-b10a-4c8b-9e42-e2a601b1c243 | 2024-02-05        | 2024-02-07          | {about,categories,index,index,contact}
 140053526121344_4                   -140053526121344_8a450e3d-2634-4e6c-9f77-e4d5e5764b0c | 2024-02-01        | 2024-02-03          | {category_2}
(20 rows)
```
You can already see that the sessions for 140053526121344_7, 140053526121344_8, 140053526121344_9 only have 3 rows each.
That's the result of the file truncate operation from the dataset preparation step. In the sessions table 
you can also see that they are considered as completed:
```
SELECT session_id, execution_time_id, pages FROM dedp.sessions ORDER BY session_id DESC;
                                        session_id                                         | execution_time_id |       pages        
-------------------------------------------------------------------------------------------+-------------------+--------------------
 140053526121344_9                   -140053526121344_288e5cc6-a22f-4ae5-88ad-a7f19a8599b9 | 2024-02-04        | {category_15,main}
 140053526121344_8                   -140053526121344_0f8220bf-1c68-4592-bf76-4a69cda55ce8 | 2024-02-04        | {main,contact}
 140053526121344_7                   -140053526121344_7bb924d2-80b3-426c-a877-d9f1e2029557 | 2024-02-04        | {home,about}
(3 rows)

```