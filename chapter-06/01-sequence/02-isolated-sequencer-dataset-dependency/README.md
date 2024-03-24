# Isolated sequencer - dataset dependency

1. Generate the dataset:
```
cd docker/dataset
mkdir -p /tmp/dedp/ch06/01-sequence/02-isolated-sequencer-dataset-dependency/input
mkdir -p /tmp/dedp/ch06/01-sequence/02-isolated-sequencer-dataset-dependency/input-internal
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
* the next operator copies the file from an external location to our storage; the location is time-partitioned
6. Open the [devices_aggregator.py](dags%2Fdevices_aggregator.py)
* the pipeline aggregates the loaded devices
* it expresses the dataset dependency with the `FileSensor` in the beginning of the pipeline
* two other steps perform the aggregation by first loading the partitioned file and later rerunning the
  aggregation query.
7. Enable the `devices_loader` and `devices_aggregator` DAGs on the UI
8. Wait for all their 3 runs to complete.
9. As a result you should see not only the loading DAG running but also the `devices_aggregator`
10. Count the rows in the _devices_aggregates_. You should see 50 rows counted three times as we've loaded 
the same dataset.

```
$ docker exec -ti dedp_ch06_01_sequencer_02 psql --user dedp_test -d dedp 
psql (11.9 (Debian 11.9-1.pgdg90+1))
Type "help" for help.


dedp=# SELECT * FROM dedp.devices_aggregates;
  type  |                    full_name                     |      version       | all_occurrences 
--------+--------------------------------------------------+--------------------+-----------------
 htc    | Evo 3d                                           | Android 12L        |               3
 lenovo | Yoga 7i (14" Intel) 2 in 1 Laptop                | Ubuntu 23          |               3
 lenovo | ThinkPad X1 Carbon Gen 10 (14" Intel) Laptop     | Ubuntu 20          |               3
 htc    | Sensation Xe                                     | Android 12         |               3
 htc    | Evo 3d                                           | Android Pie        |               3
 mac    | MacBook Air (15-inch, M2, 2023)                  | macOS Monterey     |               3
 galaxy | Galaxy Camera                                    | Android 10         |               3
 htc    | Sensation                                        | Android 14         |               3
 mac    | MacBook Pro (16-inch, 2021)                      | macOS Monterey     |               3
 galaxy | Galaxy Mini                                      | Android 10         |               3
 lg     | G2x                                              | Android 11         |               3
 lenovo | Yoga 7i (14" Intel) 2 in 1 Laptop                | v17112586409785484 |               3
 iphone | APPLE iPhone 8 Plus (Gold, 64 GB)                | iOS 14             |               3
 htc    | Evo 3d                                           | Android 15         |               3
 lg     | Nexus 4                                          | Android Pie        |               3
 iphone | APPLE iPhone 11 (White, 64 GB)                   | iOS 14             |               3
 galaxy | Galaxy Ace II                                    | Android 11         |               3
 htc    | Sensation 4g                                     | Android Pie        |               3
 mac    | MacBook Pro (14-inch, 2021)                      | macOS Monterey     |               3
 htc    | Sensation Xe                                     | Android 11         |               3
 galaxy | Galaxy Camera                                    | Android Pie        |               3
 mac    | MacBook Pro (16-inch, M3, 2023)                  | macOS Ventura      |               3
 htc    | Sensation 4g                                     | Android 12L        |               3
 galaxy | Galaxy Nexus                                     | Android 15         |               3
 lg     | Intuition                                        | v17112586409827019 |               3
 iphone | APPLE iPhone 8 Plus (Silver, 256 GB)             | iOS 17             |               3
 lenovo | ThinkBook 16 Gen 6 (16" Intel) Laptop            | Windows 10         |               3
 galaxy | Galaxy S                                         | Android 12L        |               3
 lenovo | Yoga 7i (14" Intel) 2 in 1 Laptop                | Windows 10         |               3
 galaxy | Galaxy Mini                                      | Android 15         |               3
 galaxy | Galaxy Q                                         | Android 12L        |               3
 galaxy | Galaxy S Blaze 4g                                | Android 13         |               3
 htc    | Amaze 4g                                         | Android 14         |               3
 galaxy | Galaxy S Blaze 4g                                | Android 12         |               3
 lg     | G2x                                              | Android Pie        |               3
 lg     | Spectrum                                         | Android 15         |               3
 iphone | APPLE iPhone 11 Pro Max (Midnight Green, 64 GB)  | iOS 17             |               3
 lenovo | Legion Pro 5i Gen 8 (16" Intel) Gaming Laptop    | v17112586409792663 |               3
 lenovo | ThinkPad X1 Carbon Gen 11 (14" Intel) Laptop     | Ubuntu 23          |               3
 htc    | Sensation 4g                                     | Android 12         |               3
 lg     | Intuition                                        | Android 14         |               3
 mac    | MacBook Pro (14-inch, M2, 2023)                  | macOS Sonoma       |               3
 mac    | MacBook Pro (16-inch, M3, 2023)                  | v17112586409812257 |               3
 galaxy | Galaxy Gio                                       | Android 13         |               3
 galaxy | Galaxy S 3 mini                                  | Android 12L        |               3
 lg     | Intuition                                        | Android 13         |               3
 lg     | Nexus 4                                          | Android 12L        |               3
 galaxy | Galaxy S Plus                                    | Android 14         |               3
 lenovo | ThinkPad X1 Carbon Gen 10 (14" Intel) Laptop     | Ubuntu 23          |               3
 lenovo | ThinkPad P16s Gen 2 (16" AMD) Mobile Workstation | Windows 10         |               3
(50 rows)

dedp=# SELECT COUNT(*) FROM dedp.devices_raw;
 count 
-------
   150
(1 row)
```
