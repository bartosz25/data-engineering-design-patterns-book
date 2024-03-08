# Data enrichment - dynamic joiner with Apache Flink temporal table join

1. Start Apache Kafka broker:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [VisitsWithAdsJoinJob.java](src%2Fmain%2Fjava%2Fcom%2Fbecomedataengineer%2FVisitsWithAdsJoinJob.java)
* the job defines two Kafka topics as Flink tables with
  * each declaration is composed of the table schema that is automatically resolved by Apache Flink
    thanks to the deserializer created from the `.option("value.format", "json")` option
  * besides, since we are in the streaming world, the declaration also specifies the watermark condition
* after the input tables definitions, the job initializes a `TemporalTableFunction` that is used in
  temporal table joins
* 💡 A temporal table join combines events from the left dataset with the most recent entry present
     in the right dataset. There is a nuance, though. The "most recent" is defined as the last value 
     compared to the time column of the left dataset. That's what the `adsTable.createTemporalTableFunction($("update_time"), $("ad_page"))` and `joinLateral(call("adsLookupFunction", $("event_time")),  $("ad_page").isEqual($("page")))`
     are doing:
  *  to simplify the join condition, it could be rewritten as take the most recent ad whose update_time column is lower or equal to the
     event time column
3. Start the `VisitsWithAdsJoinJob.java`
4. Run a console consumer on top of the enriched visits topic:
```
docker exec ch05_01_data_enrichment_02_dynamic_joiner_flink kafka-console-consumer.sh --topic visits-with-ads --bootstrap-server localhost:9092
```
5. Run console producer for the visits topic:
```
docker exec -ti ch05_01_data_enrichment_02_dynamic_joiner_flink kafka-console-producer.sh --topic visits --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=//
```
6. Run console producer for the ads topic:
```
docker exec -ti ch05_01_data_enrichment_02_dynamic_joiner_flink kafka-console-producer.sh --topic ads --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=//
```
7. Generate the first two visits:
```
visit-1//{"visit_id": "visit-1", "event_time": "2024-01-01T08:19:00.000Z", "page": "home"}
visit-2//{"visit_id": "visit-2", "event_time": "2024-01-01T08:21:00.000Z", "page": "home"}
```
8. Generate two versions of the same ad for the first visits:
```
home//{"ad_page": "home", "update_time": "2024-01-01T08:18:00.000Z", "campaign_name": "supermarket"}
home//{"ad_page": "home", "update_time": "2024-01-01T08:20:00.000Z", "campaign_name": "car dealer"}
```
At this moment, although the join might happen, it wasn't emitted to the sink as the watermark didn't advance:
![flink_watermark_local_time.png](assets%2Fflink_watermark_local_time.png)

(my local time zone which is UTC+01:00)

9. Produce new ad versions so that the watermark can move on:

For ads:
```
home//{"ad_page": "home", "update_time": "2024-01-01T08:22:00.000Z", "campaign_name": "empty"}
home//{"ad_page": "home", "update_time": "2024-01-01T08:29:00.000Z", "campaign_name": "car dealer"}
```

And for visits:

```
visit-3//{"visit_id": "visit-3", "event_time": "2024-01-01T08:31:00.000Z", "page": "home"}
```
Now, two matches occurred before the watermark should be written into the output topic:

```
{"visit_id":"visit-1","event_time":"2024-01-01T08:19:00Z","page":"home","ad_page":"home","campaign_name":"supermarket","update_time":"2024-01-01T08:18:00Z"}
{"visit_id":"visit-2","event_time":"2024-01-01T08:21:00Z","page":"home","ad_page":"home","campaign_name":"car dealer","update_time":"2024-01-01T08:20:00Z"}
```

As you can notice, each join in the _Temporal table join_ uses the most recent version of an entry prior
to the column defined in the lateral join expression (`visit.event_time` in our case). 

The table below illustrates this a bit better than the plain text:

| ad_page | update_time      | campaign_name |
|---------|------------------|---------------|
| home    | 2024-01-01T08:18 | supermarket   |
| home    | 2024-01-01T08:20 | car dealer    |
| home    | 2024-01-01T08:22 | empty         |
| home    | 2024-01-01T08:29 | car dealer    |


10. Let's now add some new visits that are going to match the two last entries
    (<small>watermark is 08:24AM at that moment</small>):

```
visit-4//{"visit_id": "visit-4", "event_time": "2024-01-01T08:26:00.000Z", "page": "home"}
visit-5//{"visit_id": "visit-5", "event_time": "2024-01-01T08:31:00.000Z", "page": "home"}
```
And to advance the watermark on the ads, let's add a new add not related to the home page:

```
category1//{"ad_page": "category1", "update_time": "2024-01-01T08:31:00.000Z", "campaign_name": "empty"}
```

11. The watermark moved to 08:26AM and it triggered the join for the visit-4

```
{"visit_id":"visit-4","event_time":"2024-01-01T08:26:00Z","page":"home","ad_page":"home","campaign_name":"empty","update_time":"2024-01-01T08:22:00Z"}
```

12. The visit-3 and visit-5 are still in the buffer, as the watermark didn't pass by. Let's move the watermark on the
visits stream first by adding this event:

```
visit-6//{"visit_id": "visit-6", "event_time": "2024-01-01T08:45:00.000Z", "page": "home"}
```

And let's add some ads too, but this time we're going to insert an out-of-order ad for the home page:

```
home//{"ad_page": "home", "update_time": "2024-01-01T08:28:00.000Z", "campaign_name": "grocery"}
home//{"ad_page": "home", "update_time": "2024-01-01T08:27:00.000Z", "campaign_name": "gym opening"}
category2//{"ad_page": "category2", "update_time": "2024-01-01T08:45:00.000Z", "campaign_name": "empty"}
```

Despite this out-of-order events, the join occurred for the most recent ad, added in the beginning:

```
{"visit_id":"visit-3","event_time":"2024-01-01T08:31:00Z","page":"home","ad_page":"home","campaign_name":"car dealer","update_time":"2024-01-01T08:29:00Z"}
{"visit_id":"visit-5","event_time":"2024-01-01T08:31:00Z","page":"home","ad_page":"home","campaign_name":"car dealer","update_time":"2024-01-01T08:29:00Z"}
```

13. Let's end this demo with the join happening for the category2. To do so, let's add new visits:
```
visit-7//{"visit_id": "visit-7", "event_time": "2024-01-01T08:45:00.000Z", "page": "category2"}
visit-8//{"visit_id": "visit-8", "event_time": "2024-01-01T08:55:00.000Z", "page": "category2"}
```

and an ad to advance the watermark:

```
category3//{"ad_page": "category3", "update_time": "2024-01-01T08:57:00.000Z", "campaign_name": "empty"}
```

It generates the buffered join for the visit-6 and the new join for the visit-7 made on the same event_time and update_time:

```
{"visit_id":"visit-6","event_time":"2024-01-01T08:45:00Z","page":"home","ad_page":"home","campaign_name":"car dealer","update_time":"2024-01-01T08:29:00Z"}
{"visit_id":"visit-7","event_time":"2024-01-01T08:45:00Z","page":"category2","ad_page":"category2","campaign_name":"empty","update_time":"2024-01-01T08:45:00Z"}
```