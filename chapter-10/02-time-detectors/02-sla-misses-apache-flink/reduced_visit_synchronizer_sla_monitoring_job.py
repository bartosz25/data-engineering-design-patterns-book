import os

from pyflink.common import Configuration, Time, Types, WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment, DataStream, TimeCharacteristic, \
    RuntimeExecutionMode
from pyflink.datastream.window import TumblingEventTimeWindows, EventTimeTrigger
from pyflink.table import (StreamTableEnvironment, Table)

from aggregation import extract_grouping_key, PercentilesAggregateFunction
from assigner import AppendTimeTimestampAssigner
from processor import SlaDocumentsProcessor
from window import PercentilesOutputWindowFormatter

config = Configuration()
config.set_string("classloader.resolve-order", "parent-first")
config.set_string("rest.port", "4747")
env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
env.add_jars(
    f"file://{os.getcwd()}/kafka-clients-3.7.0.jar",
    f"file://{os.getcwd()}/flink-connector-base-1.18.0.jar",
    f"file://{os.getcwd()}/flink-connector-kafka-3.1.0-1.18.jar"
)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(2)
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
env.get_config().set_auto_watermark_interval(5000)

table_environment = StreamTableEnvironment.create(env)

# In this demo we have to switch between Table and DataStream APIs as the timestamp seems to be missing in the
# KafkaSource
table_environment.execute_sql("""
CREATE TEMPORARY TABLE reduced_visits (
  `start_processing_time_ms` BIGINT,
  `append_time` TIMESTAMP METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'reduced_visits',
  'format' = 'json',
  'properties.bootstrap.servers' = 'localhost:9094',
  'properties.group.id' = 'latency_monitor',
  'scan.startup.mode' = 'earliest-offset'
)
""")

sla_query: Table = table_environment.sql_query("""
SELECT
    append_time,
    ((1000 * UNIX_TIMESTAMP(CAST(append_time AS STRING)) + EXTRACT(MILLISECOND FROM append_time)) - start_processing_time_ms)
     AS time_difference,
     FLOOR(append_time TO MINUTE) AS visit_time_minute
FROM reduced_visits
""")

sla_query_datastream: DataStream = table_environment.to_data_stream(sla_query)


windowed_result: DataStream = (sla_query_datastream
                               .assign_timestamps_and_watermarks(WatermarkStrategy.for_monotonous_timestamps()
                                  .with_idleness(Duration.of_minutes(1))
                                  .with_timestamp_assigner(AppendTimeTimestampAssigner()))
                               .key_by(extract_grouping_key)
                               .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                               .trigger(EventTimeTrigger.create())
                               .aggregate(aggregate_function=PercentilesAggregateFunction(),
                                            window_function=PercentilesOutputWindowFormatter(),
                                            accumulator_type = Types.LIST(Types.INT()),
                                            output_type = Types.STRING())

                               )

# overcoming `SinkFunction` limitations that seems to available in Java API only
windowed_result.process(SlaDocumentsProcessor())

env.execute("SLA observer for reduced visit synchronizer")