from pyflink.common import WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat

from mapper import JsonToVisitMapper


def create_json_visits_reader(env: StreamExecutionEnvironment) -> DataStream:
    json_visits_reader = (
        env.from_source(
            source=FileSource.for_record_stream_format(
                StreamFormat.text_line_format(),
                '/tmp/dedp/candidates/hybrid-continuous-consumer/apache-flink/input'
            ).monitor_continuously(Duration.of_minutes(1))
            .build(),
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="json-file-source"
        )
        # Let's reduce the parallelism here and process each file inside one thread to
        # not impact the Kafka consumer
        .set_parallelism(1)
        .map(JsonToVisitMapper())
        .set_parallelism(1)
    )
    return json_visits_reader