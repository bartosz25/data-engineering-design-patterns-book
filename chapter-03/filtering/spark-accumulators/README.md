# Filtering - programmatic API (Spark accumulators)

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch03/filtering/input
docker-compose down --volumes; docker-compose up
```
The dataset generates 1000 rows with 80% of rows with data quality issues. 
2. Explain the [devices_table_creator.py](devices_table_creator.py)
* the code creates a dynamic wrapper class (`FilterWithAccumulator`) for all filtering expressions
  * each declaration includes the filter itself but also the accumulator where negative evaluation results should be
    delivered
  * unlike a classical text-based `.where` function, whose serialization/deserialization is optimized even for Python,
    here we need to use a different construct
    * either a programmatic `filter` from `RDD` but this has a big serialization overhead
    * or a `mapInPandas` converted as a filter, which reduces the overhead by applying given transformation to multiple
      rows
* in the end all valid devices are saved as a new table and all filters stats printed
3. Run the `devices_table_creator.py`
You should see an output like:
```
type // type is null // 51
type // type is too short (1 chars or less) // 0
full_name // full_name is null // 20
version // version is null // 9
```

4. Validate the filtering by running [devices_table_reader.py](devices_table_reader.py)
You should an output like:
```
+----+---------+-------+
|type|full_name|version|
+----+---------+-------+
+----+---------+-------+

Not empty rows
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

Rows=20
Rows=20
```