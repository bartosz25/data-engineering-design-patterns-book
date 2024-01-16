# Late data detection - interception with Apache Flink
1. Explain the components:
[late_data_dispatcher_job.py](late_data_dispatcher_job.py)
```
# It says that the data source has unordered data, i.e. that the event time is not strictly increasing
watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
    .with_timestamp_assigner(VisitTimestampAssigner())
    
# ...
# Here we define the OutputTag for the side output storing late records
late_data_output: OutputTag = OutputTag('late_events', Types.STRING())

# This part transforms each record. Besides the pure mapping logic, it also classifies the records
# as being on-time or late
visits: DataStream = (kafka_data_stream.map(map_json_to_reduced_visit)
                      .process(VisitLateDataProcessor(late_data_output), Types.STRING()))
# [visit_late_data_processor.py](visit_late_data_processor.py) responsible for late data mapping
  def process_element(self, value: ReducedVisit, ctx: 'ProcessFunction.Context'):
      current_watermark = ctx.timer_service().current_watermark()
      if current_watermark > value.event_time:
          yield self.late_data_output, json.dumps(VisitWithStatus(visit=value, is_late=True).to_dict())
      else:
          yield json.dumps(VisitWithStatus(visit=value, is_late=False).to_dict())

# In the end, both records go to different sinks
visits.get_side_output(late_data_output).sink_to(kafka_sink_late_visits)
visits.sink_to(kafka_sink_valid_data)
```
2. Start Apache Kafka with the data generator:
```
cd docker
docker-compose down --volumes --remove-orphans; docker-compose up
```
3. Start the `late_data_dispatcher_job.py` 
4. Start two Kafka consumers:
* for the reduced, hence valid, visits:
  `docker exec -ti dedp_ch03_late_data_flink_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic on_time_visits`
* for the unprocessable visits:
  `docker exec -ti dedp_ch03_late_data_flink_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic late_visits`
Both consumers should get some data. You can find the examples below:

```
# late visits
{"visit": {"id": "140256709012352_50", "event_time": 1698796800}, "is_late": true}
{"visit": {"id": "140256709012352_50", "event_time": 1698797100}, "is_late": true}
{"visit": {"id": "140256709012352_50", "event_time": 1698797280}, "is_late": true}
{"visit": {"id": "140256709012352_50", "event_time": 1698797460}, "is_late": true}
{"visit": {"id": "140256709012352_50", "event_time": 1698797640}, "is_late": true}


# on-time visits
{"visit": {"id": "140256709012352_17", "event_time": 1698802800}, "is_late": false}
{"visit": {"id": "140256709012352_20", "event_time": 1698803760}, "is_late": false}
{"visit": {"id": "140256709012352_21", "event_time": 1698803280}, "is_late": false}
{"visit": {"id": "140256709012352_22", "event_time": 1698803340}, "is_late": false}
{"visit": {"id": "140256709012352_23", "event_time": 1698804120}, "is_late": false}
{"visit": {"id": "140256709012352_28", "event_time": 1698802980}, "is_late": false}
{"visit": {"id": "140256709012352_29", "event_time": 1698803100}, "is_late": false}
{"visit": {"id": "140256709012352_34", "event_time": 1698803700}, "is_late": false}
{"visit": {"id": "140256709012352_35", "event_time": 1698803220}, "is_late": false}
{"visit": {"id": "140256709012352_37", "event_time": 1698803041}, "is_late": false}
{"visit": {"id": "140256709012352_40", "event_time": 1698803400}, "is_late": false}
{"visit": {"id": "140256709012352_43", "event_time": 1698803220}, "is_late": false}
{"visit": {"id": "140256709012352_48", "event_time": 1698802927}, "is_late": false}
{"visit": {"id": "140256709012352_49", "event_time": 1698803100}, "is_late": false}
```
⚠️ You might need to wait up to 1 minute to see the late events delivered. This is due to the late data simulation 
logic of the data generator.