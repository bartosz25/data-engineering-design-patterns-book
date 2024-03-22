# Aligned Fan-in - Apache Spark UNION

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch06/02-fan-int/01-aligned-fan-in-spark/input-1/input
mkdir -p /tmp/dedp/ch06/02-fan-int/01-aligned-fan-in-spark/input-2/input
docker-compose down --volumes; docker-compose up
```

2. Explain the [aligned_fan_in_devices_union_job.py](aligned_fan_in_devices_union_job.py)
* the job implements the aligned fan-in because it performs a `UNION` operation, hence _aligns_ two different datasets
  into one Delta table

3. Run `aligned_fan_in_devices_union_job.py`

4. Run the `devices_table_reader.py`. You should see 100 rows counted and a preview of the output table:
```
All rows in the devices table=100
==> dataset_1
+------+------------------------------------------------+--------------+---------+
|type  |full_name                                       |version       |dataset  |
+------+------------------------------------------------+--------------+---------+
|mac   |MacBook Pro (16-inch, M3, 2023)                 |macOS Sonoma  |dataset_1|
|lenovo|Legion Slim 5 Gen 8 (16" AMD) Gaming Laptop     |Ubuntu 22     |dataset_1|
|galaxy|Galaxy S 3 mini                                 |Android 15    |dataset_1|
|iphone|APPLE iPhone 11 Pro Max (Midnight Green, 64 GB) |iOS 15        |dataset_1|
|galaxy|Galaxy W                                        |Android 15    |dataset_1|
|mac   |MacBook Pro (13-inch, M2, 2022)                 |macOS Sonoma  |dataset_1|
|lg    |Spectrum                                        |Android 12L   |dataset_1|
|galaxy|Galaxy Camera                                   |Android 14    |dataset_1|
|lg    |Intuition                                       |Android 12L   |dataset_1|
|mac   |MacBook Pro (14-inch, M2, 2023)                 |macOS Sonoma  |dataset_1|
|iphone|APPLE iPhone 11 Pro Max (Midnight Green, 64 GB) |iOS 13        |dataset_1|
|htc   |Sensation Xe                                    |Android 15    |dataset_1|
|htc   |Sensation 4g                                    |Android 12L   |dataset_1|
|mac   |MacBook Pro (14-inch, 2021)                     |macOS Catalina|dataset_1|
|lenovo|ThinkPad P16s Gen 2 (16" AMD) Mobile Workstation|Windows 11    |dataset_1|
|galaxy|Galaxy S II                                     |Android 10    |dataset_1|
|galaxy|Galaxy Gio                                      |Android 15    |dataset_1|
|lg    |G2x                                             |Android 13    |dataset_1|
|lenovo|Lenovo Slim Pro 7 (14" AMD) Laptop              |Ubuntu 22     |dataset_1|
|htc   |Amaze 4g                                        |Android 12    |dataset_1|
+------+------------------------------------------------+--------------+---------+
only showing top 20 rows

==> dataset_2
+------+--------------------------------------------+------------------+---------+
|type  |full_name                                   |version           |dataset  |
+------+--------------------------------------------+------------------+---------+
|lg    |Intuition                                   |Android 11        |dataset_2|
|iphone|APPLE iPhone 8 Plus (Silver, 256 GB)        |iOS 14            |dataset_2|
|lg    |Spectrum                                    |Android Pie       |dataset_2|
|mac   |MacBook Pro (16-inch, M2, 2023)             |macOS Monterey    |dataset_2|
|htc   |Amaze 4g                                    |Android 10        |dataset_2|
|galaxy|Galaxy Nexus                                |Android 14        |dataset_2|
|iphone|APPLE iPhone SE (Black, 64 GB)              |v17110877993756700|dataset_2|
|htc   |Evo 3d                                      |Android Pie       |dataset_2|
|mac   |MacBook Pro (14-inch, 2021)                 |macOS Catalina    |dataset_2|
|lg    |Nexus 4                                     |Android Pie       |dataset_2|
|lg    |Intuition                                   |v17110877993761261|dataset_2|
|lg    |Spectrum                                    |Android 12        |dataset_2|
|galaxy|Galaxy S 3 mini                             |Android 15        |dataset_2|
|htc   |Sensation Xe                                |Android 11        |dataset_2|
|iphone|APPLE iPhone 11 (White, 64 GB)              |iOS 15            |dataset_2|
|galaxy|Galaxy Ace                                  |Android 12        |dataset_2|
|lenovo|ThinkPad X1 Carbon Gen 11 (14" Intel) Laptop|Windows 11        |dataset_2|
|mac   |MacBook Air (13-inch, M2, 2022)             |macOS Big Sur     |dataset_2|
|galaxy|Galaxy Gio                                  |Android 13        |dataset_2|
|galaxy|Galaxy S Plus                               |Android 12        |dataset_2|
+------+--------------------------------------------+------------------+---------+
only showing top 20 rows
```