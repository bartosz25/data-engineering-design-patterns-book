# Deduplication - window

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch03/deduplication/input
docker-compose down --volumes; docker-compose up
```
The dataset generates 1000 rows with 50% of duplicates. 
2. Explain the [devices_deduplicator.py](devices_deduplicator.py)
* the job uses `WINDOW` function to group all related devices and take only one occurrence for each group
  * it defines a _dummy_ sort (`ORDER BY 1`) because a sort expression is required by the function
3. Run the [devices_table_reader.py](devices_table_reader.py)
You shouldn't see any duplicated rows:
```
Duplicated rows
+----+---------+-------+-----+
|type|full_name|version|count|
+----+---------+-------+-----+
+----+---------+-------+-----+

Unique rows
+------+------------------------------------------------+------------------+-----+
|type  |full_name                                       |version           |count|
+------+------------------------------------------------+------------------+-----+
|lenovo|Legion Pro 5i Gen 8 (16" Intel) Gaming Laptop   |Ubuntu 23         |1    |
|lenovo|Legion Slim 5 Gen 8 (16" AMD) Gaming Laptop     |v17051208949409700|1    |
|lenovo|ThinkPad P16s Gen 2 (16" AMD) Mobile Workstation|v17051208948819770|1    |
|lg    |Spectrum                                        |Android 12        |1    |
|lg    |Spectrum                                        |Android Pie       |1    |
|mac   |MacBook Air (13-inch, M2, 2022)                 |v17051208949391787|1    |
|htc   |Evo 3d                                          |v17051208949395788|1    |
|iphone|APPLE iPhone 12 (Black, 128 GB)                 |v17051208948668684|1    |
|lg    |G2x                                             |v17051208949174844|1    |
|galaxy|Galaxy Q                                        |Android 12        |1    |
|galaxy|Galaxy Q                                        |v17051208949557388|1    |
|htc   |Amaze 4g                                        |Android 11        |1    |
|iphone|APPLE iPhone 12 (Black, 128 GB)                 |iOS 14            |1    |
|htc   |Sensation 4g                                    |v17051208949653987|1    |
|htc   |Sensation Xe                                    |Android 12        |1    |
|lenovo|ThinkPad X1 Carbon Gen 10 (14" Intel) Laptop    |Ubuntu 23         |1    |
|lg    |G2x                                             |v17051208949645893|1    |
|galaxy|Galaxy Gio                                      |Android 11        |1    |
|htc   |Sensation                                       |v17051208949203125|1    |
|htc   |Sensation Xe                                    |v17051208949483466|1    |
+------+------------------------------------------------+------------------+-----+
only showing top 20 rows
``` 