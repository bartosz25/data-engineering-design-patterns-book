# File marker
1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch02/data-readiness/marker/input
docker-compose down --volumes; docker-compose up
```
2. Run the `load_devices_data.py` 
3. Run the `devices_parquet_reader.py`
* you should see the devices dataset converted to Apache Parquet alongside all the files
  generated by the job, including a `_SUCCESS` marker
```
Files generated by Apache Spark
File=part-00000-06911e48-6bc9-4c91-956b-643307992740-c000.snappy.parquet
File=.part-00000-06911e48-6bc9-4c91-956b-643307992740-c000.snappy.parquet.crc
File=._SUCCESS.crc
File=_SUCCESS
```