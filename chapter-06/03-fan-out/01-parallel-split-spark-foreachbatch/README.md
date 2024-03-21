# Fan-out with Apache Spark

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch06/03-fan-out/01-parallel-split-spark/input/
docker-compose down --volumes; docker-compose up
```
2. Explain the [load_devices_data.py](load_devices_data.py)
* the job is a regular Delta Lake writer but with idempotent capability, i.e. the `.option('txnVersion', batch_id).option('txnAppId', app_id)`
  * it means that any already succeeded write with these properties will not be rerun
3. Run the `load_devices_data.py`. It should fail with the exception left in the code.
4. Comment the exception in the `load_devices_data.py`.
5. Run the `load_devices_data.py`. This time it should succeed.
6. Run the `devices_table_reader.py`. It should count 50 rows (configured in data generator) each time. If the job wasn't idempotent, 
   the fan-out would lead to 100 rows in the _Raw table_ output:
```
------------- Raw table --------------
+------+-----------------------------------------------+------------------+
|type  |full_name                                      |version           |
+------+-----------------------------------------------+------------------+
|lg    |Nexus 4                                        |Android 10        |
|galaxy|Galaxy Q                                       |Android 15        |
|galaxy|Galaxy Q                                       |Android 14        |
|htc   |Sensation Xe                                   |Android Pie       |
|iphone|APPLE iPhone SE (Black, 64 GB)                 |v17109966964221454|
|htc   |Evo 3d                                         |Android 12        |
|iphone|APPLE iPhone 11 (White, 64 GB)                 |iOS 15            |
|mac   |MacBook Air (15-inch, M2, 2023)                |macOS Monterey    |
|galaxy|Galaxy Ace                                     |Android 13        |
|galaxy|Galaxy S 3 mini                                |Android 12        |
|lg    |Intuition                                      |Android 10        |
|lenovo|ThinkPad X1 Carbon Gen 11 (14" Intel) Laptop   |Ubuntu 22         |
|htc   |Amaze 4g                                       |Android 14        |
|galaxy|Galaxy S Blaze 4g                              |Android 12L       |
|htc   |Amaze 4g                                       |Android 13        |
|iphone|APPLE iPhone 11 Pro Max (Midnight Green, 64 GB)|iOS 17            |
|iphone|APPLE iPhone 8 Plus (Silver, 256 GB)           |iOS 16            |
|mac   |MacBook Air (13-inch, M2, 2022)                |macOS Catalina    |
|lenovo|Yoga 7 (16" AMD) 2-in-1 Laptop                 |Ubuntu 23         |
|iphone|APPLE iPhone 11 (White, 64 GB)                 |iOS 13            |
+------+-----------------------------------------------+------------------+
only showing top 20 rows

All rows=50
------------- Enriched table --------------
+------+------------------------------------------------------+------------------+-----------------------+
|type  |full_name                                             |version           |loading_time           |
+------+------------------------------------------------------+------------------+-----------------------+
|lg    |Nexus 4 Android 10                                    |Android 10        |2024-03-21 05:54:23.983|
|galaxy|Galaxy Q Android 15                                   |Android 15        |2024-03-21 05:54:23.983|
|galaxy|Galaxy Q Android 14                                   |Android 14        |2024-03-21 05:54:23.983|
|htc   |Sensation Xe Android Pie                              |Android Pie       |2024-03-21 05:54:23.983|
|iphone|APPLE iPhone SE (Black, 64 GB) v17109966964221454     |v17109966964221454|2024-03-21 05:54:23.983|
|htc   |Evo 3d Android 12                                     |Android 12        |2024-03-21 05:54:23.983|
|iphone|APPLE iPhone 11 (White, 64 GB) iOS 15                 |iOS 15            |2024-03-21 05:54:23.983|
|mac   |MacBook Air (15-inch, M2, 2023) macOS Monterey        |macOS Monterey    |2024-03-21 05:54:23.983|
|galaxy|Galaxy Ace Android 13                                 |Android 13        |2024-03-21 05:54:23.983|
|galaxy|Galaxy S 3 mini Android 12                            |Android 12        |2024-03-21 05:54:23.983|
|lg    |Intuition Android 10                                  |Android 10        |2024-03-21 05:54:23.983|
|lenovo|ThinkPad X1 Carbon Gen 11 (14" Intel) Laptop Ubuntu 22|Ubuntu 22         |2024-03-21 05:54:23.983|
|htc   |Amaze 4g Android 14                                   |Android 14        |2024-03-21 05:54:23.983|
|galaxy|Galaxy S Blaze 4g Android 12L                         |Android 12L       |2024-03-21 05:54:23.983|
|htc   |Amaze 4g Android 13                                   |Android 13        |2024-03-21 05:54:23.983|
|iphone|APPLE iPhone 11 Pro Max (Midnight Green, 64 GB) iOS 17|iOS 17            |2024-03-21 05:54:23.983|
|iphone|APPLE iPhone 8 Plus (Silver, 256 GB) iOS 16           |iOS 16            |2024-03-21 05:54:23.983|
|mac   |MacBook Air (13-inch, M2, 2022) macOS Catalina        |macOS Catalina    |2024-03-21 05:54:23.983|
|lenovo|Yoga 7 (16" AMD) 2-in-1 Laptop Ubuntu 23              |Ubuntu 23         |2024-03-21 05:54:23.983|
|iphone|APPLE iPhone 11 (White, 64 GB) iOS 13                 |iOS 13            |2024-03-21 05:54:23.983|
+------+------------------------------------------------------+------------------+-----------------------+
only showing top 20 rows

All rows=50
```