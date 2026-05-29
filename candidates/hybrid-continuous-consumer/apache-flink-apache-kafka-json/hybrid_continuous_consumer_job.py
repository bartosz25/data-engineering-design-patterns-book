import os
import time

from pyflink.datastream import DataStream, StreamExecutionEnvironment

from flink_configuration import prepare_execution_environment
from json_visits_reader import create_json_visits_reader
from kafka_visits_reader import create_kafka_visits_reader

os.environ['TZ'] = 'UTC'
time.tzset()

env: StreamExecutionEnvironment = prepare_execution_environment()

json_visits = create_json_visits_reader(env=env)
kafka_visits = create_kafka_visits_reader(env=env)

combined_visits: DataStream = json_visits.union(kafka_visits)

combined_visits.print('result >>')

env.execute()