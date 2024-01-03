# Passthrough replicator
## JSON replication

1. Generate the dataset and start Apache Kafka broker:
```
cd docker
mkdir -p /tmp/dedp/ch02/replication/passthrough-replicator/input
docker-compose down --volumes; docker-compose up
```
2. Open [dataset_replicator_raw.py](dataset_replicator_raw.py)
* this is the replicator for the text file formats like JSON or CSV; it doesn't use a JSON or CSV reader though 
  to avoid data altering issues
  * instead it uses the most basic `text` API
  * 💡 if you don't need a distributed processing, you can implement the pattern with copy CLI
3. Run the `dataset_reader_raw.py`
4. Verify the synchronization result by running the `dataset_reader_raw.py`

## Apache Kafka replication
1. Keep the Docker containers running and create the topic for our demo:
```
docker exec -ti passthrough_replicator_kafka kafka-topics.sh --topic events --delete --bootstrap-server localhost:9094
docker exec -ti passthrough_replicator_kafka kafka-topics.sh --topic events --create --bootstrap-server localhost:9094 --partitions 2
docker exec -ti passthrough_replicator_kafka kafka-topics.sh --topic events-replicated --delete --bootstrap-server localhost:9094
docker exec -ti passthrough_replicator_kafka kafka-topics.sh --topic events-replicated --create --bootstrap-server localhost:9094 --partitions 2
```
2. Open [dataset_replicator_kafka.py](dataset_replicator_kafka.py)
* here the replication is not that simple as for the previous example, despite of copying the data without any 
  transformation
  * the difficulty comes from the ordering that is an intrinsic part of an Apache Kafka topic; you might also 
    need to keep the order for the files from the previous example
* the job still uses the most primitive IO API for reading and writing but it decorates it with a local sorting 
  defined as `events.sortWithinPartitions('offset', ascending=True).drop('offset')`
* ⚠️ the replication may introduce some quality issues in case of retries
3. Run the `dataset_replicator_kafka.py`
4. Open [kafka_data_producer.py](kafka_data_producer.py)
* this is our data generator; we don't use the usual data-generator here to better illustrate the ordering specificity
* the job leverages record key partitioning to write all related events to the same partition
5. Run the `dataset_reader_kafka.py` and `dataset_reader_kafka_raw.py`
6. Run the `kafka_data_producer.py` a few times
* you should see the same records between the produced and replicated ones