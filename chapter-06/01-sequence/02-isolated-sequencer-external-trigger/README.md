# Isolated sequencer - task trigger

1. Generate the dataset:
```
cd docker/dataset
mkdir -p /tmp/dedp/ch06/01-sequence/02-isolated-sequencer-external-trigger/input
mkdir -p /tmp/dedp/ch06/01-sequence/02-isolated-sequencer-external-trigger/input-internal
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

5. Open the [devices_loader.py](dags%2Fdevices_loader.py)
* the DAG starts by waiting for the devices input file; an important point here is the `mode='reschedule'` attribute
  that avoids the DAG blocking the worker slots
* the next operator copies the file from an external location to our storage
* the last task triggers all downstream consumers; in our case there is only one
6. Start the `devices_loader` DAG on the UI
7. Wait for all 3 DAG runs to complete before checking the status.
8. As a result you should see not only the loading DAG running but also the `devices_aggregator`


The solution uses the `ExternalTaskMarker` in the parent pipeline to mark all depending consumers. It's particularly
useful for the backfilling purposes but it doesn't trigger the task directly. Instead, all downstream
consumers start with the `ExternalTaskSensor` referencing the parent. That way, even if a downstream consumer
gets added to the system, it can be replayed even if the parent already completed the execution.

9. Let's clean the _trigger_downstream_consumers_ task. This time, be sure to select the **Recursive** button.
Otherwise, the backfill won't apply to the downstream pipelines.
![clean_task_recursive.png](assets%2Fclean_task_recursive.png)

As a result, you should see the aggregates DAG backfilling:
![backfill_recursive.png](assets%2Fbackfill_recursive.png)

10. Count the rows in the _devices_aggregates_. You should see 50 rows counted three times as we've loaded 
the same dataset.

```
$ docker exec -ti dedp_ch06_01_sequencer_02 psql --user dedp_test -d dedp 
psql (11.9 (Debian 11.9-1.pgdg90+1))
Type "help" for help.


dedp=# SELECT * FROM dedp.devices_aggregates;
  type  |                   full_name                   |      version       | all_occurrences 
--------+-----------------------------------------------+--------------------+-----------------
 mac    | MacBook Air (15-inch, M2, 2023)               | macOS Catalina     |               3
 htc    | Sensation Xe                                  | Android 10         |               3
 lenovo | ThinkPad X1 Carbon Gen 10 (14" Intel) Laptop  | Ubuntu 20          |               3
 htc    | Sensation                                     | Android 14         |               3
 htc    | Amaze 4g                                      | v17112523116717339 |               3
 galaxy | Galaxy Mini                                   | Android 10         |               3
 lg     | Intuition                                     | Android 10         |               3
 lg     | Spectrum                                      | Android 14         |               3
 iphone | APPLE iPhone 12 Pro Max (Graphite, 128 GB)    | iOS 14             |               3
 mac    | MacBook Pro (16-inch, M2, 2023)               | macOS Monterey     |               3
 mac    | MacBook Pro (14-inch, M2, 2023)               | v17112523116723527 |               3
 htc    | Sensation 4g                                  | Android 10         |               3
 galaxy | Galaxy Y                                      | Android Pie        |               3
 lg     | Nexus 4                                       | Android 13         |               3
 htc    | Evo 3d                                        | v17112523116697739 |               3
 iphone | APPLE iPhone 12 Pro Max (Graphite, 128 GB)    | iOS 17             |               3
 lg     | G2x                                           | v17112523116708891 |               3
 htc    | Sensation 4g                                  | Android Pie        |               3
 iphone | APPLE iPhone 8 Plus (Space Grey, 256 GB)      | iOS 14             |               3
 lenovo | Legion Pro 5i Gen 8 (16" Intel) Gaming Laptop | Windows 11         |               3
 htc    | Sensation Xe                                  | Android 11         |               3
 lenovo | Legion Slim 5 Gen 8 (16" AMD) Gaming Laptop   | Windows 10         |               3
 lg     | Nexus 4                                       | Android 11         |               3
 htc    | Amaze 4g                                      | Android 13         |               3
 lenovo | Lenovo Slim Pro 7 (14" AMD) Laptop            | Ubuntu 20          |               3
 lenovo | ThinkPad X1 Carbon Gen 11 (14" Intel) Laptop  | Ubuntu 20          |               3
 htc    | Sensation                                     | Android 13         |               3
 lg     | G2x                                           | Android 14         |               3
 galaxy | Galaxy Ace                                    | Android 10         |               3
 lenovo | Yoga 7 (16" AMD) 2-in-1 Laptop                | Ubuntu 23          |               3
 iphone | APPLE iPhone 11 (White, 64 GB)                | iOS 15             |               3
 htc    | Sensation 4g                                  | Android 13         |               3
 htc    | Sensation Xe                                  | Android 13         |               3
 mac    | MacBook Pro (16-inch, M2, 2023)               | macOS Sonoma       |               3
 htc    | Sensation                                     | Android 15         |               3
 mac    | MacBook Pro (16-inch, M2, 2023)               | macOS Big Sur      |               3
 mac    | MacBook Pro (13-inch, M1, 2020)               | macOS Ventura      |               3
 mac    | MacBook Pro (13-inch, M2, 2022)               | macOS Catalina     |               3
 lg     | Intuition                                     | Android 15         |               3
 iphone | APPLE iPhone 8 (Silver, 256 GB)               | iOS 16             |               3
 mac    | MacBook Air (M1, 2020)                        | macOS Monterey     |               3
 galaxy | Galaxy S 4g                                   | Android Pie        |               3
 lg     | G2x                                           | Android 15         |               3
 lenovo | ThinkBook 16 Gen 6 (16" Intel) Laptop         | Ubuntu 20          |               3
 lg     | Spectrum                                      | Android 12         |               3
 mac    | MacBook Pro (14-inch, M2, 2023)               | macOS Sonoma       |               3
 iphone | APPLE iPhone 12 (Black, 128 GB)               | iOS 16             |               3
 lenovo | Lenovo Slim Pro 7 (14" AMD) Laptop            | Ubuntu 22          |               3
 lenovo | Yoga 7i (14" Intel) 2 in 1 Laptop             | Ubuntu 22          |               3
 htc    | Evo 3d                                        | Android 12         |               3
(50 rows)

dedp=# SELECT COUNT(*) FROM dedp.devices_raw;
 count 
-------
   150
(1 row)
```
