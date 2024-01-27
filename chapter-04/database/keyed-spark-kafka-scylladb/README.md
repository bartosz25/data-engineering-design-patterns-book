# Database - keyed idempotency
1. Start Docker containers:
```
cd docker;
docker-compose down --volumes; docker-compose up
```
2. Start a Kafka producer:
```
docker exec -ti dedp_ch04_keyed_kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic visits
```
3. Initialize ScyllaDB:
```
docker exec dedp_ch04_scylla cqlsh -f /init.cql
```
3. Explain the [session_sgenerator_job.py](sessions_generator_job.py)
* stateful job that applies a grouping logic for the events;
  * typically, we could solve it much easier if the input dataset had a reliable _visit id_ attribute
* explain the [visits_mapper.py](visits_mapper.py)
  * the logic is a simple data accumulation of visited pages
  * besides, it also stores the append time for the first processed record; it'll be used to generate
    the session id
    * we're using here this technical attribute instead
      of the event time **because of the stronger idempotency guarantee**
      * put differently, the session id will not be impacted if after a failure, 
        we restart the job and get some late data with the event time earlier than the processed 
        one - append time will remain the same as it's a strictly increasing number
4. Go to the console producer and add the first visit:
```
{"user_id": 1, "page": "A.html", "event_time": "2023-02-20T03:03:20"}
```
5. Start the `sessions_generator_job.py`
6. Add two new visits:
```
{"user_id": 1, "page": "B.html", "event_time": "2023-02-20T03:03:26"}
{"user_id": 1, "page": "C.html", "event_time": "2023-02-20T03:05:26"}
```
It should close the session and write it to ScyllaDB.
7. Check the sessions in the table:
```
$ docker exec -ti dedp_ch04_scylla cqlsh
Connected to  at 192.168.112.3:9042.
[cqlsh 5.0.1 | Cassandra 3.0.8 | CQL spec 3.3.1 | Native protocol v4]
Use HELP for help.
cqlsh> SELECT * FROM dedp.sessions;

 session_id          | user_id | ingestion_time                  | pages
---------------------+---------+---------------------------------+--------------------------------
 4249952624734199298 |       1 | 2024-01-27 06:20:45.422000+0000 | ['A.html', 'B.html', 'C.html']

(1 rows)
```

8. Clean the checkpoint location and restart the job:
```
rm -rf /tmp/dedp/ch04/database/keyed-kafka-spark
```

9. Add a new record to move the stream forward:
```
{"user_id": 2, "page": "D.html", "event_time": "2023-02-20T04:55:26"}
```

10. The new visit should re-trigger the output generation for the user_id=1. Since the session id is 
idempotent, you should still see only one record for user_id=1 in the table. 
This time, the ingestion time will be different, though:
```
cqlsh> SELECT * FROM dedp.sessions;

 session_id          | user_id | ingestion_time                  | pages
---------------------+---------+---------------------------------+--------------------------------
 4249952624734199298 |       1 | 2024-01-27 06:22:46.328000+0000 | ['A.html', 'B.html', 'C.html']
 2355054627897975025 |       2 | 2024-01-27 06:22:46.328000+0000 |                     ['D.html']

(2 rows)
```