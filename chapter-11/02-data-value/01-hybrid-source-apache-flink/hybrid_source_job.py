from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import BasicTypeInfo
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.datastream.connectors.hybrid_source import HybridSource
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

from flink_configuration import prepare_execution_environment

env = prepare_execution_environment()
base_dir = '/tmp/dedp/ch11/11-streaming/01-hybrid-source-apache-flink/input'

switch_timestamp = KafkaOffsetsInitializer.earliest()
file_source = (FileSource
               .for_record_stream_format(StreamFormat.text_line_format(), base_dir)
               .build())

kafka_source = (KafkaSource
                .builder()
                .set_bootstrap_servers('localhost:9094')
                .set_group_id('visits_reader')
                .set_value_only_deserializer(SimpleStringSchema())
                .set_topics('visits')
                .set_starting_offsets(switch_timestamp)
                .build())

hybrid_source = HybridSource.builder(file_source).add_source(kafka_source).build()

env.from_source(hybrid_source, WatermarkStrategy.no_watermarks(), 'hybrid_visits',
                type_info=BasicTypeInfo.STRING_TYPE_INFO()).print()

env.execute()
