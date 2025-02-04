 # Horizontal partitioning - custom partitioner with Apache Kafka
 
1. Start Apache Kafka broker:
```
cd docker/
docker-compose down --volumes; docker-compose up
```
2. Explain the [RangePartitioner.java](src%2Fmain%2Fjava%2Fcom%2Fwaitingforcode%2FRangePartitioner.java)
* the partitioner simulates range partitioning by sending [A-B] to partition 0, [C-D] to 1, and so forth
  * there is also a default partitioner that delivers all other letters to the partition 4
    * you might see it already, if there is a lot of "default" records, the partition 4 will be 
      overloaded
3. Explain the [DataProducerWithRangePartitioner.java](src%2Fmain%2Fjava%2Fcom%2Fwaitingforcode%2FDataProducerWithRangePartitioner.java)
* that's a very basic data producer
* the most important part here is how it sets the partitioner class via the `partitioner.class` property
4. Run `DataProducerWithRangePartitioner`
* it should stop after generating all the records
5. Check on what partitions have landed the produced records by running the console consumer.
You should see an output like:
```
$ docker exec dedp_kafka_broker kafka-console-consumer.sh --topic letters --bootstrap-server localhost:9092 --from-beginning --property parse.key=true --property print.partition=true

Partition:4	Value=Z
Partition:0	Value=A
Partition:0	Value=A
Partition:1	Value=D
Partition:2	Value=E
```

⚠️ Your producer may get stuck if you try to send a record to a non existing partition.