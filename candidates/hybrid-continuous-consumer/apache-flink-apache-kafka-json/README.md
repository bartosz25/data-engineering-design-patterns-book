# Hybrid continuous consumer with Apache Flink, Apache Kafka, and JSON

> [!NOTE]
> If you encounter some installation issues, run this inside your virtual environment:
> ```
> pip install -r requirements.txt --no-build-isolation
> ```
--

1. Prepare the historical dataset:
```shell
rm -rf /tmp/dedp/candidates/hybrid-continuous-consumer/apache-flink
mkdir -p /tmp/dedp/candidates/hybrid-continuous-consumer/apache-flink/input
```

2. Explain [hybrid_continuous_consumer_job.py](hybrid_continuous_consumer_job.py)
* the job runs a Kafka consumer with the default parallelism and JSON consumer with a static
parallelism set to 1

3. Start Docker image with Apache Kafka and data generator:
```shell
cd dataset
docker-compose down --volumes; docker-compose up
```

4. Create two files with the data:
```shell
echo '{"visit_id": "batch1", "user_id": "user1", "page": "home.html"}' >> /tmp/dedp/candidates/hybrid-continuous-consumer/apache-flink/input/file1.json
echo '{"visit_id": "batch1", "user_id": "user2", "page": "contact.html"}' >> /tmp/dedp/candidates/hybrid-continuous-consumer/apache-flink/input/file1.json
echo '{"visit_id": "batch2", "user_id": "user1", "page": "basket.html"}' >> /tmp/dedp/candidates/hybrid-continuous-consumer/apache-flink/input/file2.json
echo '{"visit_id": "batch2", "user_id": "user2", "page": "confirmation.html"}' >> /tmp/dedp/candidates/hybrid-continuous-consumer/apache-flink/input/file2.json
```

4. Run the `hybrid_continuous_consumer_job.py`
You should see the logs printing only one mapping task for JSON and 2 mapping tasks for Kafka:
```
Started the mapping ('Source: json-file-source -> Map (1/1)#0',) — subtask 0 of 1
Started the mapping ('Source: Input Visits from Kafka -> Extract-Timestamp -> Timestamps/Watermarks -> Remove-Timestamp -> Map (2/3)#0',) — subtask 1 of 3
Started the mapping ('Source: Input Visits from Kafka -> Extract-Timestamp -> Timestamps/Watermarks -> Remove-Timestamp -> Map (3/3)#0',) — subtask 2 of 3
```

Later on you can see the printer sink generating both real-time and batch records:
```
result >>:2> Visit(visit_id='batch1', user_id='user2', event_time=None, page='contact.html')
result >>:3> Visit(visit_id='batch2', user_id='user1', event_time=None, page='basket.html')
result >>:1> Visit(visit_id='batch1', user_id='user1', event_time=None, page='home.html')
result >>:1> Visit(visit_id='batch2', user_id='user2', event_time=None, page='confirmation.html')
result >>:2> Visit(visit_id='214965274926262080_2', user_id='274926262080_56a29b83-7eb7-4929-9bf4-33120aed6009', event_time=1777593600000, page='home')
result >>:3> Visit(visit_id='46489274926262080_0', user_id='274926262080_e284a550-eded-4dd4-9e9c-c10ec47bbcc5', event_time=1777593600000, page='about')
result >>:2> Visit(visit_id='214965274926262080_2', user_id='274926262080_56a29b83-7eb7-4929-9bf4-33120aed6009', event_time=1777593840000, page='page_7')
result >>:2> Visit(visit_id='214965274926262080_2', user_id='274926262080_56a29b83-7eb7-4929-9bf4-33120aed6009', event_time=1777594020000, page='page_17')
result >>:3> Visit(visit_id='214486274926262080_1', user_id='274926262080_a8f564bc-8da0-407d-b16f-dd94feb32ff1', event_time=1777593600000, page='index')
```
