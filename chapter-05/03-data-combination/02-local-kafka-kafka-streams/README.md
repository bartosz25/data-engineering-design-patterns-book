# Data combination - local Spark with Kafka partitions

1. Start Apache Kafka broker and generate the dataset:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [VisitsLocalAggregatorJob.java](src%2Fmain%2Fjava%2Fcom%2Fwaitingforcode%2FVisitsLocalAggregatorJob.java)
* the crucial part here is the `groupByKey` operation that must be called instead of `groupBy` because the latter
  involves shuffle; i.e. it's a way of saying, "I want to apply a custom key, so obviously it'll impact the input
  data partitioning"; this StackOverflow answer is pretty self-explanatory: https://stackoverflow.com/a/65181874/9726075
  * besides, you'll find some explanation in the ["Mastering Kafka Streams and ksqlDB"](https://www.oreilly.com/library/view/mastering-kafka-streams/9781492062486/),
    chapter 4
4. Run a console consumer on top of the aggregated visits topic:
```
docker exec dedp_ch05_local_kafkastreams_kafka kafka-console-consumer.sh --topic visits-aggregated --bootstrap-server localhost:9092
```

After some seconds you should see the first records coming in:
```
140074269006720_0	{"visitId":"140074269006720_0","pages":["category_16","home","category_14","main","home","about","about","home","index","contact","page_15"]}
140074269006720_1	{"visitId":"140074269006720_1","pages":["main","contact","category_13","contact","contact","home","contact","categories","category_18","index","page_1"]}
140074269006720_2	{"visitId":"140074269006720_2","pages":["categories","categories","home","category_15","main","categories","categories","about","home","contact","categories"]}
140074269006720_3	{"visitId":"140074269006720_3","pages":["home","category_10","contact","contact","about","page_9","main","page_6","index","page_12","category_15"]}
140074269006720_4	{"visitId":"140074269006720_4","pages":["index","categories","contact","page_2","main","category_19","categories","page_19","page_3","categories","page_2"]}
140074269006720_0	{"visitId":"140074269006720_0","pages":["category_16","home","category_14","main","home","about","about","home","index","contact","page_15","categories","contact","page_6"]}
140074269006720_1	{"visitId":"140074269006720_1","pages":["main","contact","category_13","contact","contact","home","contact","categories","category_18","index","page_1","contact","main","index"]}
140074269006720_2	{"visitId":"140074269006720_2","pages":["categories","categories","home","category_15","main","categories","categories","about","home","contact","categories","page_13","main","contact"]}
140074269006720_3	{"visitId":"140074269006720_3","pages":["home","category_10","contact","contact","about","page_9","main","page_6","index","page_12","category_15","contact","main","main"]}
140074269006720_4	{"visitId":"140074269006720_4","pages":["index","categories","contact","page_2","main","category_19","categories","page_19","page_3","categories","page_2","index","contact","page_13"]}
140074269006720_0	{"visitId":"140074269006720_0","pages":["category_16","home","category_14","main","home","about","about","home","index","contact","page_15","categories","contact","page_6","about","page_16"]}
```
