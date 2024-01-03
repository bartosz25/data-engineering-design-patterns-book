# Full loader - intermediary data exposition

1. Generate the dataset:
```
cd docker/dataset
mkdir -p /tmp/dedp/ch02/full-loader/data-exposition/input
docker-compose down --volumes; docker-compose up
```
2. Start a PostgreSQL instance:
```
cd ../postgresql
docker-compose down --volumes; docker-compose up
```
3. Start Apache Airflow instance:
**⚠️ Enable the Virtual Environment before**
```
./start.sh
```
4. Access the Web UI at http://localhost:8080/login/ with dedp/dedp as login/password
5. Go to the `devices_loader` DAG at http://localhost:8080/dags/devices_loader/grid?tab=graph
* the pipeline loads devices file to our PostgreSQL instance
* ⚠️ the DAG focuses on the data ingestion part and doesn't include any data quality-related steps;
   you'll see them in the **Data Value** chapter
6. Open the [devices_loader.py](dags%2Fdevices_loader.py)
* the DAG starts by waiting for the devices input file; an important point here is the `mode='reschedule'` attribute
  that avoids the DAG blocking the worker slots
* the next operator first creates a new table timestamped with the execution time; later, it loads the devices from the
  input file to this table
  * at this moment, the loaded data remains hidden
* in the final step, the hidden table gets promoted to the public view with the `CREATE OR REPLACE VIEW devices`
  operator
7. Start the `devices_loader` DAG on the UI
8. Wait for all 3 DAG runs to complete and check the devices view.
9. Create an empty dataset file:
```
echo "type;full_name;version" > /tmp/dedp/ch02/full-loader/data-exposition/input/dataset.csv
```
10. Restart the last DAG run by clicking on the "Clear existing tasks"
![clear_tasks.png](assets%2Fclear_tasks.png)
11. Check the devices view:
```
$ docker exec -ti dedp_test_full_loader_postgresql psql --user dedp_test -d dedp_test 
psql (11.9 (Debian 11.9-1.pgdg90+1))
Type "help" for help.

dedp_test=# SELECT * FROM devices;
 type | full_name | version 
------+-----------+---------
(0 rows)
```
11. Restore the previous version by restarting the `load_data_to_the_final_table` task from the previous DAG instance:
![restart_clear.png](assets%2Frestart_clear.png)
12. Check again the view; you should see now the devices from the previous instance's load:
```
$ docker exec -ti dedp_test_full_loader_postgresql psql --user dedp_test -d dedp_test 
psql (11.9 (Debian 11.9-1.pgdg90+1))
Type "help" for help.

dedp_test=# SELECT * FROM devices;
  type  |                    full_name                     |      version       
--------+--------------------------------------------------+--------------------
 htc    | Sensation 4g                                     | Android 12
 mac    | MacBook Pro (13-inch, M1, 2020)                  | macOS Monterey
 lg     | G2x                                              | Android 14
 mac    | MacBook Pro (14-inch, M2, 2023)                  | macOS Catalina
 lg     | Nexus 4                                          | Android 10
...
 mac    | MacBook Pro (13-inch, M2, 2022)                  | macOS Monterey
 galaxy | Galaxy S Plus                                    | Android 15
 lenovo | ThinkBook 13s Gen 4 (13" AMD) Laptop             | Ubuntu 23
 lenovo | Yoga 7i (14" Intel) 2 in 1 Laptop                | Ubuntu 22
 lenovo | Legion Slim 5 Gen 8 (16" AMD) Gaming Laptop      | Ubuntu 23
(50 rows)
```
