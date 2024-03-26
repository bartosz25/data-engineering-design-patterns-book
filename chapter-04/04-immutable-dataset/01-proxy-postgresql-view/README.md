# Proxy - PostgreSQL view
1. Prepare the directory with the data:
```
mkdir -p /tmp/dedp/ch04/proxy/input
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
* the pipeline has two steps:
  * _load_data_to_internal_table_ to load devices file into an internal (hidden) table
  * _refresh_view_ to refresh a view exposed to the end users with the most recent loaded table
  * the pipeline implements the Proxy pattern as it exposes the dataset via an intermediary structure 
6. Run the `devices_loader`. It should run only once.
7. Check the results in the database:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
dedp=# \dt dedp.*
                       List of relations
 Schema |               Name               | Type  |   Owner   
--------+----------------------------------+-------+-----------
 dedp   | devices_internal_20240120_121542 | table | dedp_test
(1 row)

dedp=# SELECT COUNT(*) FROM dedp.devices_internal_20240120_121542;
 count 
-------
    50
(1 row)

dedp=# SELECT * FROM dedp.devices_internal_20240120_121542 LIMIT 5;
  type  |                   full_name                   |    version    
--------+-----------------------------------------------+---------------
 lenovo | ThinkPad X1 Carbon Gen 11 (14" Intel) Laptop  | Ubuntu 22
 lenovo | Legion Pro 5i Gen 8 (16" Intel) Gaming Laptop | Ubuntu 23
 mac    | MacBook Air (M1, 2020)                        | macOS Ventura
 htc    | Sensation Xe                                  | Android 12
 iphone | APPLE iPhone 8 Plus (Silver, 256 GB)          | iOS 14
(5 rows)

dedp=# SELECT COUNT(*) FROM dedp.devices;
 count 
-------
    50
(1 row)

dedp=# SELECT * FROM dedp.devices LIMIT 5;
  type  |                   full_name                   |    version    
--------+-----------------------------------------------+---------------
 lenovo | ThinkPad X1 Carbon Gen 11 (14" Intel) Laptop  | Ubuntu 22
 lenovo | Legion Pro 5i Gen 8 (16" Intel) Gaming Laptop | Ubuntu 23
 mac    | MacBook Air (M1, 2020)                        | macOS Ventura
 htc    | Sensation Xe                                  | Android 12
 iphone | APPLE iPhone 8 Plus (Silver, 256 GB)          | iOS 14
(5 rows)


```
You should see exactly one table and one view exposing it.
8. Replace the dataset file with:
```
echo "type;full_name;version
mac;MacBook Air (M1, 2020);macOS Ventura
mac;MacBook Pro (14-inch, 2021);macOS Ventura
mac;MacBook Pro (13-inch, M1, 2020);macOS Sonoma
mac;MacBook Air (13-inch, M2, 2022);macOS Catalina" > /tmp/dedp/ch04/proxy/input/dataset.csv
```
9. Restart the pipeline and check the results again. You should see a new table and refreshed view:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
dedp=# \dt dedp.*
                       List of relations
 Schema |               Name               | Type  |   Owner   
--------+----------------------------------+-------+-----------
 dedp   | devices_internal_20240120_121542 | table | dedp_test
 dedp   | devices_internal_20240120_124238 | table | dedp_test
(2 rows)


dedp=# SELECT COUNT(*) FROM dedp.devices_internal_20240120_124238;
 count 
-------
     4
(1 row)


dedp=# SELECT * FROM dedp.devices_internal_20240120_124238;
 type |            full_name            |    version     
------+---------------------------------+----------------
 mac  | MacBook Air (M1, 2020)          | macOS Ventura
 mac  | MacBook Pro (14-inch, 2021)     | macOS Ventura
 mac  | MacBook Pro (13-inch, M1, 2020) | macOS Sonoma
 mac  | MacBook Air (13-inch, M2, 2022) | macOS Catalina
(4 rows)

dedp=# SELECT COUNT(*) FROM dedp.devices;
 count 
-------
    4
(1 row)

dedp=# SELECT * FROM dedp.devices LIMIT 5;
 type |            full_name            |    version     
------+---------------------------------+----------------
 mac  | MacBook Air (M1, 2020)          | macOS Ventura
 mac  | MacBook Pro (14-inch, 2021)     | macOS Ventura
 mac  | MacBook Pro (13-inch, M1, 2020) | macOS Sonoma
 mac  | MacBook Air (13-inch, M2, 2022) | macOS Catalina
(4 rows)
```
