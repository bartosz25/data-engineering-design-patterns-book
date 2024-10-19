# Full loader

1. Generate the dataset:
```
cd ../docker
mkdir -p /tmp/dedp/ch02/full-loader/input
docker-compose down --volumes; docker-compose up
```
2. Run [load_json_data.py](load_json_data.py)
* the job loads JSON data to a Delta Lake table
* although it looks straightforward, it's important to define the input schema for all semi-structured formats, like
  `JSON` or `CSV`. Otherwise, Apache Spark tries to infers the schema from the dataset by processing it twice

3. Run [devices_table_reader.py](devices_table_reader.py) to read the loaded data.
4. Run [load_json_partial_data.py](load_json_partial_data.py)
* the job loads an empty dataset, hence overwrites the whole table
5. Run [devices_table_reader_past_version.py](devices_table_reader_past_version.py)
* the job uses the `option('versionAsOf', '0')` helper to restore the previous version of the dataset
