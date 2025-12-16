import os

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode


def prepare_execution_environment() -> StreamExecutionEnvironment:
    config = Configuration()
    config.set_string("classloader.resolve-order", "parent-first")
    config.set_string("rest.port", "4747")
    config.set_string('table.local-time-zone', "UTC")
    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.add_jars(
        f"file://{os.getcwd()}/kafka-clients-3.9.1.jar",
        f"file://{os.getcwd()}/flink-connector-base-2.1.0.jar",
        f"file://{os.getcwd()}/flink-connector-kafka-4.0.1-2.0.jar"
    )
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.get_config().set_auto_watermark_interval(5000)

    return env
