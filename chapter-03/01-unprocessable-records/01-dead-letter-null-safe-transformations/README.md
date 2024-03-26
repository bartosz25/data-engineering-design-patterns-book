# Dead-Letter - batch

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch03/dead-letter/input
docker-compose down --volumes; docker-compose up
```
2. Explain the [devices_loader.py](devices_loader.py)
* the job creates a new column in the input dataset; the column uses a `CONCAT` function that returns `null`
  if one of the columns is missing
* in the second part, the job classifies each record as being "valid" or "dead-lettered" to write them
  to corresponding tables
3. Run the [devices_dead_letter_table_reader.py](devices_dead_letter_table_reader.py)
You should see some Dead-Lettered rows:
```
+------+-------------------------------------+------------------+-----------------+
|type  |full_name                            |version           |name_with_version|
+------+-------------------------------------+------------------+-----------------+
|htc   |NULL                                 |NULL              |NULL             |
|NULL  |Galaxy S Blaze 4g                    |NULL              |NULL             |
|galaxy|NULL                                 |Android 12        |NULL             |
|mac   |NULL                                 |macOS Big Sur     |NULL             |
|NULL  |NULL                                 |macOS Ventura     |NULL             |
|NULL  |Yoga 7i (14" Intel) 2 in 1 Laptop    |NULL              |NULL             |
|NULL  |Yoga 7 (16" AMD) 2-in-1 Laptop       |NULL              |NULL             |
|mac   |MacBook Air (15-inch, M2, 2023)      |NULL              |NULL             |
...
|htc   |Sensation 4g                         |NULL              |NULL             |
|mac   |NULL                                 |v17051178459631982|NULL             |
+------+-------------------------------------+------------------+-----------------+
only showing top 20 rows

```
4. Run the [devices_table_reader.py](devices_table_reader.py)
You should see some valid rows too:
```
+------+-------------------------------------------+-----------+-----------------------------------------------------+
|type  |full_name                                  |version    |name_with_version                                    |
+------+-------------------------------------------+-----------+-----------------------------------------------------+
|htc   |Amaze 4g                                   |Android 12L|Amaze 4gAndroid 12L                                  |
|lenovo|Legion Slim 5 Gen 8 (16" AMD) Gaming Laptop|Windows 11 |Legion Slim 5 Gen 8 (16" AMD) Gaming LaptopWindows 11|
|galaxy|Galaxy S II                                |Android 14 |Galaxy S IIAndroid 14                                |
...
|lg    |G2x                                        |Android 14 |G2xAndroid 14                                        |
|htc   |Sensation                                  |Android 13 |SensationAndroid 13                                  |
|htc   |Evo 3d                                     |Android 13 |Evo 3dAndroid 13                                     |
|NULL  |APPLE iPhone 11 (White, 64 GB)             |iOS 13     |APPLE iPhone 11 (White, 64 GB)iOS 13                 |
|NULL  |APPLE iPhone 8 (Silver, 256 GB)            |iOS 15     |APPLE iPhone 8 (Silver, 256 GB)iOS 15                |
+------+-------------------------------------------+-----------+-----------------------------------------------------+
only showing top 20 rows
```
You can notice that the output contains rows with `NULL` type and it's not a bug. 
From our Dead-Lettering logic, it's still a valid record.