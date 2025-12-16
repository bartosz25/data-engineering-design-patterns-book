from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, StringType

from config import OUTPUT_TOPIC_NAME
from schema import EVENT_SCHEMA
from visit_duration_stateful_processor import VisitsDurationCalculator

spark_session = (SparkSession.builder.master('local[*]')
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0')
    .config('spark.sql.streaming.stateStore.providerClass',
            'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider')
    .config('spark.sql.shuffle.partitions', 2).getOrCreate())

input_data = (spark_session.readStream.format('kafka')
    .option('kafka.bootstrap.servers', 'localhost:9094')
    .option("subscribe", 'visits')
    .option('startingOffsets', 'EARLIEST')
    .load())

grouped_visits = (input_data.selectExpr('CAST(value AS STRING)')
    .select(F.from_json(F.col('value'), EVENT_SCHEMA)
            .alias('data'))
    .select('data.*').withWatermark('event_time', '15 minutes')
    .groupBy(F.col('user_id')))

visits_counters = grouped_visits.transformWithStateInPandas(
    statefulProcessor=VisitsDurationCalculator(is_reprocessing=False),
    outputStructType=VisitsDurationCalculator.OUTPUT_SCHEMA,
    timeMode='eventTime',
    outputMode='update',
).selectExpr('TO_JSON(STRUCT(*)) AS value')

write_query = (visits_counters.writeStream.format('kafka')
               .option('topic', OUTPUT_TOPIC_NAME)
               .option('kafka.bootstrap.servers', 'localhost:9094')
               .option('checkpointLocation',
                       '/tmp/dedp/ch11/03-data-reprocessing/02-batch-reprocessor-stateful-session-window-apache-spark-delta-lake/checkpoint')
               .start())

write_query.awaitTermination()
