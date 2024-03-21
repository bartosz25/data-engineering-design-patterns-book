# Local sequencer with Apache Spark

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch06/01-sequence/01-local-sequencer-spark/input/
docker-compose down --volumes; docker-compose up
```
2. Explain the [load_devices_data_with_sequencer.py](load_devices_data_with_sequencer.py)
* the job implements the local sequencer by including individual steps into the processing
  * 💡 even though they're defined separately, it doesn't mean Spark will run them separately; as these filters
       apply to the same data, Spark will run them together. So no need to combine the filters on your own. As a result
       you can keep the code readable
  * 💡 besides the readability, dividing the steps also adds some flexibility. As you can see, we're using here
       different APIs for the filtering (SQL and DataFrame)
3. Run the `load_devices_data_with_sequencer.py`. It should add data to the devices table
4. Run the `devices_table_reader.py`. It should return the table with the valid data from the filters standpoint:
```
+------+-------------------------------------------------------+------------------+
|type  |full_name                                              |version           |
+------+-------------------------------------------------------+------------------+
|iphone|APPLE iPhone 8 Plus (Space Grey, 256 GB) iOS 17        |iOS 17            |
|lenovo|Legion Slim 5 Gen 8 (16" AMD) Gaming Laptop Ubuntu 22  |Ubuntu 22         |
|lenovo|Legion Slim 5 Gen 8 (16" AMD) Gaming Laptop Windows 10 |Windows 10        |
|lenovo|ThinkPad X1 Carbon Gen 11 (14" Intel) Laptop Ubuntu 22 |Ubuntu 22         |
|iphone|APPLE iPhone 8 Plus (Gold, 64 GB) iOS 13               |iOS 13            |
|lenovo|Yoga 7 (16" AMD) 2-in-1 Laptop Ubuntu 23               |Ubuntu 23         |
|lenovo|ThinkPad X1 Carbon Gen 10 (14" Intel) Laptop Windows 11|Windows 11        |
|lenovo|Yoga 7 (16" AMD) 2-in-1 Laptop Ubuntu 22               |Ubuntu 22         |
|galaxy|Galaxy S 3 mini Android 15                             |Android 15        |
|galaxy|Galaxy S 3 mini Android Pie                            |Android Pie       |
|galaxy|Galaxy S II Android Pie                                |Android Pie       |
|iphone|APPLE iPhone 11 (White, 64 GB) iOS 13                  |iOS 13            |
|galaxy|Galaxy S 4g Android 11                                 |Android 11        |
|galaxy|Galaxy Mini Android 15                                 |Android 15        |
|lenovo|ThinkBook 13s Gen 4 (13" AMD) Laptop Windows 10        |Windows 10        |
|galaxy|Galaxy Ace Android 13                                  |Android 13        |
|iphone|APPLE iPhone 8 (Silver, 256 GB) v17109989333927691     |v17109989333927691|
|iphone|APPLE iPhone 8 (Silver, 256 GB) iOS 14                 |iOS 14            |
|lenovo|Legion Pro 5i Gen 8 (16" Intel) Gaming Laptop Ubuntu 20|Ubuntu 20         |
|lenovo|Yoga 7i (14" Intel) 2 in 1 Laptop Windows 10           |Windows 10        |
+------+-------------------------------------------------------+------------------+
only showing top 20 rows
```