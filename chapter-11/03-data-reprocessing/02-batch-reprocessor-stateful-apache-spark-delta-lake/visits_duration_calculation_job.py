from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType

from visit_duration_stateful_processor import VisitsDurationCalculator

spark_session = (SparkSession.builder.master("local[*]")
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0')
    .config('spark.sql.streaming.stateStore.providerClass',
            'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider')
    .config('spark.sql.shuffle.partitions', 2).getOrCreate())

input_data = spark_session.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", 'visits') \
    .option("startingOffsets", "EARLIEST") \
    .load()

grouped_visits = (input_data.selectExpr('CAST(value AS STRING)')
    .select(F.from_json(F.col('value'), 'event_id STRING, visit_id INT, event_time TIMESTAMP, page STRING')
            .alias('data'))
    .select('data.*').withWatermark('event_time', '15 minutes')
    .groupBy(F.col('visit_id')))

# TODO: mention that the hybrid source can leverage the stateInitialization !!!! if the processing logic is stateful
visits = grouped_visits.transformWithStateInPandas(
    statefulProcessor=VisitsDurationCalculator(),
    outputStructType=StructType([
        StructField('visit_id', IntegerType()), StructField('visited_pages', IntegerType()),
        StructField('is_finished', BooleanType())
    ]) ,
    #eventTimeColumnName='event_time',
    timeMode='eventTime',
    outputMode='update',

)

write_query = (visits.writeStream.outputMode('update')
    .option('checkpointLocation', '/tmp/dedp/ch11/11-streaming/02-early-writer-apache-spark-structured-streaming/checkpoint')
    .format('console').option('truncate', False)
    .start())

write_query.awaitTermination()
