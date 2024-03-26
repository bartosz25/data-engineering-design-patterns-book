# Transactions - PostgreSQL
1. Prepare the dataset:
```
mkdir -p /tmp/dedp/ch04/transactions/input
echo "type;full_name;version
galaxy;Galaxy Camera;Android 11
iphone;APPLE iPhone 8 Plus (Silver, 256 GB);iOS 13
htc;Evo 3d;Android 12L" > /tmp/dedp/ch04/transactions/input/dataset_1.csv
echo "type;full_name;version
htc11111111;Evo 3d;Android 52L" > /tmp/dedp/ch04/transactions/input/dataset_too_long_type.csv
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
5. Explain the [devices_loader.py](dags%2Fdevices_loader.py)
* the pipeline runs two SQL queries to load the data
  * even though it could be solved with a single process, I'm creating two separate processes on purpose
    * they're running within the same transaction and the latter will fail due to a too long column value; as a
      result you shouldn't see the first transaction succeed
6. Run the `devices_loader`. It should run only once.
It should fail with the following exception:
```
[2024-01-27, 05:04:01 UTC] {taskinstance.py:1937} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/bartosz/workspace/.venvs/airflow-postgresql/lib/python3.8/site-packages/airflow/providers/common/sql/operators/sql.py", line 282, in execute
    output = hook.run(
  File "/home/bartosz/workspace/.venvs/airflow-postgresql/lib/python3.8/site-packages/airflow/providers/common/sql/hooks/sql.py", line 392, in run
    self._run_command(cur, sql_statement, parameters)
  File "/home/bartosz/workspace/.venvs/airflow-postgresql/lib/python3.8/site-packages/airflow/providers/common/sql/hooks/sql.py", line 437, in _run_command
    cur.execute(sql_statement)
psycopg2.errors.StringDataRightTruncation: value too long for type character varying(6)
CONTEXT:  COPY changed_devices_file2, line 2, column type: "htc11111111"
```
7. Check the results in the database:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
dedp=# SELECT * FROM dedp.devices;
 type | full_name | version 
------+-----------+---------
(0 rows)

```

8. Fix the invalid dataset:
```
echo "type;full_name;version
htc;Evo 3d;Android 52L" > /tmp/dedp/ch04/transactions/input/dataset_too_long_type.csv
```

9. Restart the pipeline and check the results again. You should see data correctly loaded to the table:
```
dedp=# SELECT COUNT(*) FROM dedp.devices;
 count 
-------
     4
(1 row)

dedp=# SELECT * FROM dedp.devices;
  type  |              full_name               |   version   
--------+--------------------------------------+-------------
 galaxy | Galaxy Camera                        | Android 11
 iphone | APPLE iPhone 8 Plus (Silver, 256 GB) | iOS 13
 htc    | Evo 3d                               | Android 12L
 htc    | Evo 3d                               | Android 52L
(4 rows)
```
