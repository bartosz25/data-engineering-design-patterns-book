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
3. Run the `devices_loader.py`
4. Run the [devices_dead_letter_table_reader.py](devices_dead_letter_table_reader.py)
You should see some Dead-Lettered rows:
```
+------+----------------------------------------+-----------+-----------------+
|type  |full_name                               |version    |name_with_version|
+------+----------------------------------------+-----------+-----------------+
|NULL  |Lenovo Slim Pro 7 (14" AMD) Laptop      |NULL       |NULL             |
|iphone|NULL                                    |iOS 16     |NULL             |
|NULL  |NULL                                    |Ubuntu 23  |NULL             |
|lg    |Intuition                               |NULL       |NULL             |
|mac   |MacBook Air (13-inch, M2, 2022)         |NULL       |NULL             |
|htc   |NULL                                    |Android 10 |NULL             |
|lg    |NULL                                    |Android 12 |NULL             |
|lg    |NULL                                    |Android 12L|NULL             |
|NULL  |Galaxy Mini                             |NULL       |NULL             |
|NULL  |Nexus 4                                 |NULL       |NULL             |
|iphone|NULL                                    |iOS 16     |NULL             |
|NULL  |APPLE iPhone 8 Plus (Space Grey, 256 GB)|NULL       |NULL             |
|NULL  |ThinkBook 16 Gen 6 (16" Intel) Laptop   |NULL       |NULL             |
|lenovo|NULL                                    |Windows 11 |NULL             |
|NULL  |NULL                                    |Android 12L|NULL             |
|mac   |MacBook Pro (13-inch, M2, 2022)         |NULL       |NULL             |
|NULL  |APPLE iPhone 8 Plus (Gold, 64 GB)       |NULL       |NULL             |
|galaxy|NULL                                    |Android 11 |NULL             |
|NULL  |NULL                                    |Android 12L|NULL             |
|NULL  |NULL                                    |Windows 10 |NULL             |
+------+----------------------------------------+-----------+-----------------+
only showing top 20 rows

```
5. Run the [devices_table_reader.py](devices_table_reader.py)
You should see some valid rows too:
```
+----+--------------------------------------------+------------------+------------------------------------------------------+
|type|full_name                                   |version           |name_with_version                                     |
+----+--------------------------------------------+------------------+------------------------------------------------------+
|NULL|MacBook Air (15-inch, M2, 2023)             |macOS Sonoma      |MacBook Air (15-inch, M2, 2023)macOS Sonoma           |
|NULL|MacBook Pro (14-inch, M2, 2023)             |macOS Ventura     |MacBook Pro (14-inch, M2, 2023)macOS Ventura          |
|NULL|MacBook Air (13-inch, M2, 2022)             |macOS Sonoma      |MacBook Air (13-inch, M2, 2022)macOS Sonoma           |
|NULL|MacBook Pro (16-inch, M2, 2023)             |macOS Ventura     |MacBook Pro (16-inch, M2, 2023)macOS Ventura          |
|NULL|Galaxy Mini                                 |Android 11        |Galaxy MiniAndroid 11                                 |
|NULL|Galaxy W                                    |v17312182303665311|Galaxy Wv17312182303665311                            |
|NULL|MacBook Pro (14-inch, M3, 2023)             |v17312182303672531|MacBook Pro (14-inch, M3, 2023)v17312182303672531     |
|NULL|ThinkBook 16 Gen 6 (16" Intel) Laptop       |Ubuntu 20         |ThinkBook 16 Gen 6 (16" Intel) LaptopUbuntu 20        |
|NULL|Yoga 7 (16" AMD) 2-in-1 Laptop              |Windows 11        |Yoga 7 (16" AMD) 2-in-1 LaptopWindows 11              |
|NULL|Galaxy Q                                    |Android 10        |Galaxy QAndroid 10                                    |
|NULL|ThinkBook 13s Gen 4 (13" AMD) Laptop        |Windows 10        |ThinkBook 13s Gen 4 (13" AMD) LaptopWindows 10        |
|NULL|ThinkPad X1 Carbon Gen 10 (14" Intel) Laptop|Windows 11        |ThinkPad X1 Carbon Gen 10 (14" Intel) LaptopWindows 11|
|NULL|MacBook Pro (14-inch, M2, 2023)             |v17312182303763192|MacBook Pro (14-inch, M2, 2023)v17312182303763192     |
|NULL|MacBook Air (M1, 2020)                      |v17312182303784923|MacBook Air (M1, 2020)v17312182303784923              |
|NULL|Sensation Xe                                |Android Pie       |Sensation XeAndroid Pie                               |
|NULL|Galaxy Q                                    |v17312182303802260|Galaxy Qv17312182303802260                            |
|NULL|MacBook Pro (16-inch, M2, 2023)             |macOS Catalina    |MacBook Pro (16-inch, M2, 2023)macOS Catalina         |
|NULL|Spectrum                                    |Android 13        |SpectrumAndroid 13                                    |
|NULL|Amaze 4g                                    |Android 14        |Amaze 4gAndroid 14                                    |
|NULL|Sensation 4g                                |v17312182303884803|Sensation 4gv17312182303884803                        |
+----+--------------------------------------------+------------------+------------------------------------------------------+
only showing top 20 rows
```
You can notice that the output contains rows with `NULL` type and it's not a bug. 
From our Dead-Lettering logic, it's still a valid record.