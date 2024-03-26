# Filtering - programmatic API (Spark accumulators)

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch03/filtering/input
docker-compose down --volumes; docker-compose up
```
The dataset generates 1000 rows with 80% of rows with data quality issues. 
2. Explain the [devices_table_creator.py](devices_table_creator.py)
* the code converts each filter expression into an alias with the applied filter code or `NULL`, if the row is fully valid
* in the second part, it prints, just for the informational purposes, the stats of the applied filters 
* finally, the job writes all valid rows to a dedicated table
3. Run the `devices_table_creator.py`
You should see an output like:
```
+--------+--------------+
|count(1)|   status_flag|
+--------+--------------+
|       9|  null_version|
|      20|null_full_name|
|      51|     null_type|
+--------+--------------+
```
4. Validate the filtering by running [devices_table_reader.py](devices_table_reader.py)
You should an output like:
```
Invalid rows
+----+---------+-------+
|type|full_name|version|
+----+---------+-------+
+----+---------+-------+

Valid rows
+------+------------------------------------+------------------+
|type  |full_name                           |version           |
+------+------------------------------------+------------------+
|galaxy|Galaxy S II                         |Android 12L       |
|htc   |Amaze 4g                            |Android 10        |
|mac   |MacBook Pro (16-inch, 2021)         |macOS Big Sur     |
|htc   |Sensation 4g                        |Android 12        |
|htc   |Sensation 4g                        |Android 15        |
|htc   |Evo 3d                              |Android 12        |
|mac   |MacBook Pro (14-inch, M3, 2023)     |macOS Big Sur     |
|lenovo|ThinkBook 13s Gen 4 (13" AMD) Laptop|Windows 11        |
|galaxy|Galaxy S Plus                       |Android 10        |
|htc   |Sensation Xe                        |Android 14        |
|lg    |Spectrum                            |Android 12        |
|mac   |MacBook Pro (14-inch, 2021)         |macOS Catalina    |
|galaxy|Galaxy Mini                         |Android 15        |
|htc   |Sensation Xe                        |v17052072021678665|
|iphone|APPLE iPhone 11 (White, 64 GB)      |iOS 16            |
|htc   |Amaze 4g                            |Android Pie       |
|mac   |MacBook Pro (16-inch, 2021)         |macOS Catalina    |
|iphone|APPLE iPhone 11 (White, 64 GB)      |iOS 17            |
|galaxy|Galaxy S Plus                       |Android 14        |
|mac   |MacBook Air (13-inch, M2, 2022)     |v17052072021689210|
+------+------------------------------------+------------------+
```