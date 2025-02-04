# Offline observer - Apache Spark with Apache Kafka

1. Start Apache Kafka broker with data generator:
```
mkdir -p /tmp/dedp/chapter-09/03-quality-observation/01-offline-observer-apache-spark-apache-kafka/profiles/
cd dataset
docker-compose down --volumes; docker-compose up
```

2. Explain the [passthrough_visits_processor_job.py](passthrough_visits_processor_job.py):
* it's the job we're going to observe in this demo
* it does nothing but copying records from one Apache Kafka topic to another

3. Start `passthrough_visits_processor_job.py`

4. Explain the [data_observation_job.py](data_observation_job.py):
* the job performs three types of observations:
  * data quality - it analyzes the emptiness of some fields; the fields are important for our downstream consumers
    but since they're JSON messages, we don't have a way to enforce their quality
  * job lag - it analyzes the progress of the `passthrough_visits_processor_job` by comparing the offsets from 
    the checkpoint storage with the most recent offsets in the topic
  * dataset profile - it analyzes the shape of the current `DataFrame` and creates its profile with _ydata-profiling_ library
    The profile is later saved as an HTML report.
* upon collecting all observations, it writes them to an Elastcisearch index

5. Start `data_observation_job.py`

6. Go to Grafana (http://localhost:3000/login; admin/admin) and add a new Elasticsearch data source:
* URL: http://elasticsearch:9200 
* Index name: visits_observation_stats
* Time field name: @timestamp
* Elasticsearch version: 8.0+

![es_config.png](assets/es_config.png)

After clicking on "Save & test", you should see the following:
![es_config_ok.png](assets/es_config_ok.png)

5. Let's now create some dashboards from "Add new panel". The first will show the distribution of errors 
per minute.

The query should look like:
![errors_minute.png](assets/errors_minute.png)

6. Let's also add the second dashboard that will show the evolving lag:

As you can see, there is a processing skew and one partition is late:
![offset_lag.png](assets/offset_lag.png)


7. As the last part of our offline observation let's add an alert that will trigger 
whenever there is a lag bigger than 15 offsets for a 5 minutes period:
 
![lag_alert.png](assets/lag_alert.png)

8. Besides, you should also notice dataset profile reports generated under `/tmp/dedp/chapter-09/03-quality-observation/01-offline-observer-apache-spark-apache-kafka/profiles/`, such as:
![profile.png](assets/profile.png)