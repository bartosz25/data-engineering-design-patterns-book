# Transformation replicator
## Dataset reduction
1. Generate the dataset and start Apache Kafka broker:
```
cd dataset
mkdir -p /tmp/dedp/ch02/replication/transformation-replicator/input
docker-compose down --volumes; docker-compose up
```
2. Run the `prepare_delta_table.py` to prepare the replicated Delta Lake table.
3. Open [dataset_replicator_raw_reduction.py](dataset_replicator_raw_reduction.py)
* the job uses the `drop` function to remove a column that cannot be replicated
  * as a result, it creates a new table with only 2 columns
4. Run the `dataset_replicator_raw_reduction.py`
5. Run the `dataset_reader_reduction.py` to see the replicated table:
```
+------+-----------------+
|  type|          version|
+------+-----------------+
|    lg|       Android 14|
|   htc|       Android 10|
|   mac|     macOS Sonoma|
|galaxy|       Android 13|
...
```

## Dataset transformation
1. Open the [dataset_replicator_raw_transformation.py](dataset_replicator_raw_transformation.py)
* the replicator uses a SQL function to drops the first character of the _full name_ column
  * eventually, it could be expressed as a mapping function (`mapInPandas`, `mapInArrow`) but for a one-column 
    transformation a SQL expression is easier to do
2. Run the `dataset_replicator_raw_transformation.py`
3. Run the `dataset_reader_transformation.py` to see the replicated table:
```
+------+--------------------+-----------------+
|  type|           full_name|          version|
+------+--------------------+-----------------+
|    lg|             pectrum|       Android 14|
|   htc|             maze 4g|       Android 10|
|   mac|acBook Pro (14-in...|     macOS Sonoma|
|galaxy|         alaxy Nexus|       Android 13|
...
```