# Horizontal partitioning - Apache Spark with Delta Lake and JSON

1. Explain the [records_partitioner_delta_lake_job.py](records_partitioner_delta_lake_job.py)
* it's a regular job template that will partition records by the _change_time_ column
  * you're going to see the same logic for both Delta Lake and JSON output, so I'm explaining it only once
* the job defines the partitioning schema as _year/month/day/hour_ by creating dedicated partitioning columns
  from the _change_time_ attribute
  * if we used the _change_time_ directly, the partitioning would be based on the whole column and as a result,
    it would create partitions like:
```
$ tree dedp/ch08/01-partitioning/01-horizontal-partitioner-apache-spark/delta-users/ -A
dedp/ch08/01-partitioning/01-horizontal-partitioner-apache-spark/delta-users/
├── change_date=2024-05-25 05%3A00%3A00
│   └── part-00004-69e6137b-d7b1-4015-8e1f-8487d0daeb50.c000.snappy.parquet
├── change_date=2024-05-25 10%3A00%3A00
│   ├── part-00001-a6e39bd3-0a88-4a73-a7aa-570dc7a16979.c000.snappy.parquet
│   ├── part-00002-1ac64bfe-6e41-4635-8627-d9648cbcc796.c000.snappy.parquet
│   └── part-00003-9d892ad7-5ec3-493d-94f9-4cf0e3c5e6d8.c000.snappy.parquet
├── change_date=2024-05-26 03%3A00%3A00
│   └── part-00005-8ea0e275-1d3e-4eb5-a805-f7011d712dbf.c000.snappy.parquet
├── change_date=2024-05-27 10%3A00%3A00
│   └── part-00006-fbbbf784-5bb9-48d0-b6db-790707c34a82.c000.snappy.parquet
├── change_date=2024-05-27 11%3A00%3A00
│   └── part-00007-7aede183-7bb8-4c56-b2c5-48c78fd911cb.c000.snappy.parquet
└── _delta_log
    └── 00000000000000000000.json
```

Although it works, it could be more useful to divide the partitioning granularity at the storage level so that
the consumers can leverage different query patterns, such as yearly, monthly, daily, or even hourly. For that reason,
the job creates dedicated, fine-grained, partitioning columns.

3. Run the `records_partitioner_delta_lake_job.py`

2. Explain the [records_writer_job.py](records_partitioner_delta_lake_job.py)
* the job generates new users; the partitioning logic is a bit hidden as it relies on the partitions 
  management in Apache Kafka which is based on the _key_ attribute of the produced record
  * as you can see, for some users the producer generates several rows
    * consequently, these rows should land on the same Kafka partition

3. Run the `records_partitioner_delta_lake_job.py`

4. Run the `reader_delta_lake.py`. You should see:
```
+-------+-------+-------------------+----+-----+---+----+
|user_id|country|change_date        |year|month|day|hour|
+-------+-------+-------------------+----+-----+---+----+
|3      |the USA|2024-05-25 10:00:00|2024|5    |25 |10  |
|1      |Poland |2024-05-25 10:00:00|2024|5    |25 |10  |
|2      |France |2024-05-25 10:00:00|2024|5    |25 |10  |
|2      |Poland |2024-05-27 10:00:00|2024|5    |27 |10  |
|4      |France |2024-05-27 11:00:00|2024|5    |27 |11  |
|1      |Spain  |2024-05-26 03:00:00|2024|5    |26 |3   |
|4      |Spain  |2024-05-25 05:00:00|2024|5    |25 |5   |
+-------+-------+-------------------+----+-----+---+----+
```

As you can see, the table includes the partition values. Besides, they
are also present in the _commit logs_ that makes them pretty useful for all kind of optimizations, such as 
partitioning pushdown to select only one partition's files if the query targets only a specific partition:
```
$ less /tmp/dedp/ch08/01-partitioning/01-horizontal-partitioner-apache-spark/delta-users/_delta_log/00000000000000000000.json

{"commitInfo":{"timestamp":1716520566287,"operation":"WRITE",
"operationParameters":{"mode":"Overwrite",
--> "partitionBy":"[\"year\",\"month\",\"day\",\"hour\"]"},"isolationLevel":"Serializable","isBlindAppend":false,"operationMetrics":{"numFiles":"7","numOutputRows":"7","numOutputBytes":"7294"},"engineInfo":"Apache-Spark/3.5.0 Delta-Lake/3.0.0","txnId":"827a4f5b-ce38-412d-9036-589bd39f90ab"}}

{"metaData":{"id":"0cc54de7-1fa7-4a3d-b93f-c5fce5cdef20","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"user_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"country\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"change_date\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"year\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"month\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hour\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
--> "partitionColumns":["year","month","day","hour"],"configuration":{},"createdTime":1716520561896}}

{"add":{"path":"year=2024/month=5/day=25/hour=10/part-00001-8d198b3e-50d6-4c64-984a-3647a8cd6de8.c000.snappy.parquet",
--> "partitionValues":{"year":"2024","month":"5","day":"25","hour":"10"},"size":1043,"modificationTime":1716520564809,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"user_id\":1,\"country\":\"Poland\",\"change_date\":\"2024-05-25T10:00:00.000+02:00\"},\"maxValues\":{\"user_id\":1,\"country\":\"Poland\",\"change_date\":\"2024-05-25T10:00:00.000+02:00\"},\"nullCount\":{\"user_id\":0,\"country\":0,\"change_date\":0}}"}}
```

5. Since the raw file formats, such as JSON, don't have a logical companion files like Delta Lake's commit log, 
they rely on a pure physical partitioning. Let's run the `records_partitioner_json_job.py`.

6. Run `reader_json.py`. It should print:
```
+-----------------------------+-------+-------+----+-----+---+----+
|change_date                  |country|user_id|year|month|day|hour|
+-----------------------------+-------+-------+----+-----+---+----+
|2024-05-25T10:00:00.000+02:00|the USA|3      |2024|5    |25 |10  |
|2024-05-25T10:00:00.000+02:00|Poland |1      |2024|5    |25 |10  |
|2024-05-25T10:00:00.000+02:00|France |2      |2024|5    |25 |10  |
|2024-05-27T11:00:00.000+02:00|France |4      |2024|5    |27 |11  |
|2024-05-27T10:00:00.000+02:00|Poland |2      |2024|5    |27 |10  |
|2024-05-25T05:00:00.000+02:00|Spain  |4      |2024|5    |25 |5   |
|2024-05-26T03:00:00.000+02:00|Spain  |1      |2024|5    |26 |3   |
+-----------------------------+-------+-------+----+-----+---+----+
```

However, due to the lack of the metadata layer, the partitioning is defined only at the physical layer and 
it's up to the query engine to evaluate the storage layout before deciding to apply or not the partition-based
optimizations.

