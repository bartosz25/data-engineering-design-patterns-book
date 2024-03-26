# Transactions - Apache Flink with Apache Kafka
1. Explain the [visits_reducer_job.py](visits_reducer_job.py):
* the logic is less important that the checkpoint and delivery modes, both set to the _exactly-once_
* it means that Apache Flink will commit Apache Kafka transaction after successfully creating the checkpoint, hence
  avoiding reprocessing the same data at restart
2. Start Apache Kafka with the data generator:
```
cd docker
docker-compose down --volumes --remove-orphans; docker-compose up
```
3. Start the `late_data_dispatcher_job.py` 
4. Start a Kafka consumer for the output topic:
```
docker exec -ti dedp_ch04_transactional_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --isolation-level=read_committed --topic reduced_visits
```
5. Start a Kafka consumer for the output topic with _read_uncommitted_ property:
```
docker exec -ti dedp_ch04_transactional_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --isolation-level=read_uncommitted --topic reduced_visits
```
The property acts as a marker to ignore the transactional semantics, i.e. the consumer sees all records, even
for the transactions in progress. As a consequence, if the transaction rollbacks, the consumer will process
data which is not valid. This kind of data is called _dirty reads_.
6. Start a Kafka producer for the input topic and generate some valid records:
```
docker exec -ti dedp_ch04_transactional_kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic visits
```

```
{"visit_id": "1ABC", "event_time": "2023-11-20T10:00:00.000+00:00"}
{"visit_id": "1ABC", "event_time": "2023-11-20T10:00:44.000+00:00"}
{"visit_id": "1ABC", "event_time": "2023-11-20T10:02:05.000+00:00"}
{"visit_id": "1ABC", "event_time": "2023-11-20T10:23:15.000+00:00"}
```
7. Start the `visits_reducer_job`.
8. Check the messages received by both consumers. Only the consumer with _read_uncommitted_ level
should see the records.
9. Produce a _poisson pill_ message that will fail the job:
```
fff
```
10. Check the messages received by both consumers again. 
* consumer with _read_uncommitted_ level
```
$ docker exec -ti dedp_ch04_transactional_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --isolation-level=read_uncommitted --topic reduced_visits
{"visit_id": "1ABC", "event_time": 1700474400}
{"visit_id": "1ABC", "event_time": 1700474444}
{"visit_id": "1ABC", "event_time": 1700474525}
{"visit_id": "1ABC", "event_time": 1700475795}
```
* consumer with _read_committed_ level
```
$ docker exec -ti dedp_ch04_transactional_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --isolation-level=read_committed --topic reduced_visits

```