# Fan-out with Apache Spark

## Input split
1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/input/
docker-compose down --volumes; docker-compose up
```
2. Explain the [exlusive_choice_input_argument_job.py](exlusive_choice_input_argument_job.py)
* it's the first job implementation for the exclusive choice pattern
  * as you can see, it accepts an _output_type_ parameter that configures its behavior
    * if set to _delta_lake_, the job uses a Delta Lake `SparkSession` and corresponding output configuration;
      otherwise it uses a regular `SparkSession` to generate CSV files
    * how does this switch work? It relies on a _factory design pattern_ from software engineering. The implementation
      is present in the [output_generation_factory.py](output_generation_factory.py) with 2 methods:
      * `get_spark_session` to get the `SparkSession` configuration adapted to the output type
      * `write_devices_data` to write the `DataFrame` with a good format
3. First, run the job with the following parameters:
```
python exlusive_choice_input_argument_job.py --input_dir /tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/input/ \
 --output_dir /tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/output-csv/ --output_type csv 
```

4. You should see the CSV files generated:
```
tree /tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/output-csv/ -A

/tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/output-csv/
├── part-00000-2097677b-d129-44f0-873f-e4590fba2ff1-c000.csv
└── _SUCCESS

0 directories, 2 files
```

5. Run the job for Delta Lake this time:
```
python exlusive_choice_input_argument_job.py --input_dir /tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/input/ \
 --output_dir /tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/output-delta/ --output_type delta 
```

6. You should see the Delta table files generated:
```
tree /tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/output-delta/ -A

/tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/output-delta/
├── _delta_log
│   └── 00000000000000000000.json
└── part-00000-3e32ee06-7ca5-4b5d-ad8d-f5ca659e38f1-c000.snappy.parquet

1 directory, 2 files
```

## Data split
1. Let's see now how to adapt the behavior from the input data. Analyze the [exlusive_choice_dataset_attribute.py](exlusive_choice_dataset_attribute.py):
* the job reads the input dataset to infer the schema 
  * if the schema has 3 columns or more, we consider it as a new dataset that should be written in a different
    place than the legacy schema
  * this approach uses the dataset to dynamically adapt the behavior; it has a drawback here, though as 
    inferring the schema in Apache Spark requires passing over the input dataset twice. As we've a small 
    dataset here, it's not a big deal but for more challenging scenarios, you may need to analyze a single file
    instead in a separate schema resolution step (only if schema is consistent across the files)
2. Run the `exlusive_choice_dataset_attribute.py`
3. You should see the data generated for the new table:
```
tree /tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/output/devices-table-schema-changed -A

/tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/output/devices-table-schema-changed
├── _delta_log
│   └── 00000000000000000000.json
└── part-00000-6c11ab21-c879-419b-9b21-74874b0ddd69-c000.snappy.parquet

1 directory, 2 files
```