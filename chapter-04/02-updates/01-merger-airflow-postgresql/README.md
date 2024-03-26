# Merger - PostgreSQL
1. Prepare the dataset:
```
mkdir -p /tmp/dedp/ch04/merger/input
echo "type;full_name;version
galaxy;Galaxy Camera;Android 11
iphone;APPLE iPhone 8 Plus (Silver, 256 GB);iOS 13
htc;Evo 3d;Android 12L" > /tmp/dedp/ch04/merger/input/dataset.csv
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
5. Explain the [idempotency_merge.py](dags%2Fidempotency_merge.py)
* the pipeline runs a job that loads input file and merges it with the existing one
* the merge query deals with updated values (full_name column) or new rows
6. Run the `devices_loader`. It should run only once.
7. Check the results in the database:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
dedp=# SELECT COUNT(*) FROM dedp.devices;
 count 
-------
     3
(1 row)


dedp=# SELECT * FROM dedp.devices;
  type  |              full_name               |   version   
--------+--------------------------------------+-------------
 galaxy | Galaxy Camera                        | Android 11
 iphone | APPLE iPhone 8 Plus (Silver, 256 GB) | iOS 13
 htc    | Evo 3d                               | Android 12L
(3 rows)
```

8. Replace the dataset file with:
```
echo "type;full_name;version
galaxy;Galaxy Camera V2;Android 11
iphone;APPLE iPhone 8 Plus (Silver, 256 GB);iOS 13
mac;MacBook Air (M1, 2020);macOS Ventura
mac;MacBook Pro (13-inch, M1, 2020);macOS Sonoma
htc;Evo 3d;Android 12L" > /tmp/dedp/ch04/merger/input/dataset.csv
```

9. Restart the pipeline and check the results again. You should see one updated row and two added ones; you
can run the pipeline multiple times to see there won't be any duplicates:
```
dedp=# SELECT COUNT(*) FROM dedp.devices;
 count 
-------
     5
(1 row)

dedp=# SELECT * FROM dedp.devices;
  type  |              full_name               |    version    
--------+--------------------------------------+---------------
 galaxy | Galaxy Camera V2                     | Android 11
 iphone | APPLE iPhone 8 Plus (Silver, 256 GB) | iOS 13
 mac    | MacBook Air (M1, 2020)               | macOS Ventura
 mac    | MacBook Pro (13-inch, M1, 2020)      | macOS Sonoma
 htc    | Evo 3d                               | Android 12L
(5 rows)
```
