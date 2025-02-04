# Horizontal partitioning - Apache Spark with Apache Kafka

1. Start Apache Kafka broker:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [records_writer_job.py](records_writer_job.py)
* the job generates new users; the partitioning logic is a bit hidden as it relies on the partitions 
  management in Apache Kafka which is based on the _key_ attribute of the produced record
  * as you can see, for some users the producer generates several rows
    * consequently, these rows should land on the same Kafka partition

3. Run the `records_writer_job.py`

4. Run the `records_reader_job.py`. It should print the partition information as follows:

```
+---------+---+---------------------------------+
|partition|key|value                            |
+---------+---+---------------------------------+
|0        |2  |{"user_id":2,"country":"France"} |
|0        |2  |{"user_id":2,"country":"Poland"} |
|1        |4  |{"user_id":4,"country":"France"} |
|1        |3  |{"user_id":3,"country":"the USA"}|
|1        |1  |{"user_id":1,"country":"Poland"} |
|1        |4  |{"user_id":4,"country":"Spain"}  |
|1        |1  |{"user_id":1,"country":"Spain"}  |
+---------+---+---------------------------------+
```

⚠️ The table also shows how bad data distribution can be. Remember, the logic here is a simple math not focused
on balancing the load.