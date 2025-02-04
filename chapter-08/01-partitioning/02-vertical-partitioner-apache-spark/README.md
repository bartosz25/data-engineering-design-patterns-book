# Partitioning - vertical partitioner with Apache Spark

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch08/01-partitioning/02-vertical-partitioner-apache-spark/input
docker-compose down; docker-compose up
```

2. Explain the [vertical_partitioner.py](vertical_partitioner.py)
* the job partitions the raw visits into three separate datasets: visits, users, and technical
* an important thing here is the `.cache()` call that avoids reading the input dataset three times
* in the partitioning logic, we're using an explicit projection (`select` functions) or removal (`drop` function)
  of columns not expected to be in the dataset
* finally, the users and technical data writers ensure there is only one instance for each visit
  by calling the `dropDuplicates`

3. Run the `vertical_partitioner.py`

4. Check the partitioned datasets by running `partitioned_tables_reader.py`. You should see 
one user and technical row for each visit, like for example below:

```
+------------------+--------------+------------+-------------------+----------------------------------------------------+
|visit_id          |ip            |login       |connected_since    |user_id                                             |
+------------------+--------------+------------+-------------------+----------------------------------------------------+
|140221868030848_0 |123.66.112.236|audrey00    |NULL               |140221868030848_7b8805eb-10c3-40db-82fb-4ace267df0b8|
|140221868030848_1 |11.61.72.228  |johnsonemily|2023-11-18 01:00:00|140221868030848_7268b001-ea03-42d4-892a-cc7906467488|
|140221868030848_10|105.77.51.167 |abigail16   |NULL               |140221868030848_610f89d4-e743-48f1-9243-7e56f956fb12|
+------------------+--------------+------------+-------------------+----------------------------------------------------+
only showing top 3 rows

⌛ Reading technical
+------------------+-------+---------------+------------+-----------+--------------+
|visit_id          |browser|browser_version|network_type|device_type|device_version|
+------------------+-------+---------------+------------+-----------+--------------+
|140221868030848_0 |Firefox|22.0           |5G          |PC         |2.0           |
|140221868030848_1 |Chrome |20.2           |4G          |iPhone     |1.0           |
|140221868030848_10|Chrome |25.09          |5G          |Smartphone |2.0           |
+------------------+-------+---------------+------------+-----------+--------------+
only showing top 3 rows

⌛ Reading visits
+-----------------+-------------------+------+------------------+
|visit_id         |event_time         |page  |context           |
+-----------------+-------------------+------+------------------+
|140221868030848_0|2023-11-24 01:00:00|main  |{Google Ads, ad 4}|
|140221868030848_0|2023-11-24 01:01:00|page_7|{Google Ads, ad 4}|
|140221868030848_1|2023-11-24 01:00:00|page_4|{Medium, NULL}    |
+-----------------+-------------------+------+------------------+
only showing top 3 rows
```