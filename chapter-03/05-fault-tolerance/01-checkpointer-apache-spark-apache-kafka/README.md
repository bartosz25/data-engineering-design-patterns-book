# Fault-tolerance - micro-batch

1. Explain the [visits_json_synchronizer.py](visits_json_synchronizer.py)
* the code runs a simple streaming job
  * the fault-tolerance comes form this option `.option('checkpointLocation', f'{base_dir}/checkpoint')`
  * Apache Spark will write all processed offset in each micro-batch under this location in one file per micro-batch
    numbers
2. Start the data generator with Apache Kafka:
```
cd docker
docker-compose down --volumes; docker-compose up
```
3. Start the `visits_json_synchronizer.py`
After 1-2 minutes you should see the checkpoint files created:
```
$ tree /tmp/dedp/ch03/fault-tolerance/micro-batch/checkpoint -A
/tmp/dedp/ch03/fault-tolerance/micro-batch/checkpoint
├── commits
│   ├── 0
│   ├── 1
│   ├── 10
│   ├── 11
│   ├── 12
│   ├── 13
│   ├── 14
│   ├── 15
│   ├── 16
│   ├── 17
│   ├── 18
│   ├── 2
│   ├── 3
│   ├── 4
│   ├── 5
│   ├── 6
│   ├── 7
│   ├── 8
│   └── 9
├── metadata
├── offsets
│   ├── 0
│   ├── 1
│   ├── 10
│   ├── 11
│   ├── 12
│   ├── 13
│   ├── 14
│   ├── 15
│   ├── 16
│   ├── 17
│   ├── 18
│   ├── 2
│   ├── 3
│   ├── 4
│   ├── 5
│   ├── 6
│   ├── 7
│   ├── 8
│   └── 9
└── sources
    └── 0
        └── 0
        
$ cat /tmp/dedp/ch03/fault-tolerance/micro-batch/checkpoint/offsets/18
v1
{"batchWatermarkMs":0,"batchTimestampMs":1705249601108,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"2"}}
{"visits":{"1":1276,"0":1224}}

$ cat /tmp/dedp/ch03/fault-tolerance/micro-batch/checkpoint/commits/18
v1
{"nextBatchWatermarkMs":0}
```
Anytime the job will start, it will restart either from the next offset or the last not fully processed.