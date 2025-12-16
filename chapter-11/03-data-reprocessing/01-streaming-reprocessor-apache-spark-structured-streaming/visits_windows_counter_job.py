from pyspark.sql import SparkSession, functions as F

from config import get_checkpoint_dir

spark_session = (SparkSession.builder.master("local[*]")
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0')
    .config('spark.sql.streaming.stateStore.providerClass',
            'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider')
    .config('spark.sql.shuffle.partitions', 2).getOrCreate())

input_data = (spark_session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("subscribe", 'visits')
    .option("startingOffsets", "EARLIEST")
    .load())

aggregation_query = (input_data.selectExpr('CAST(value AS STRING)')
    .select(F.from_json(F.col('value'), 'event_id STRING, visit_id INT, event_time TIMESTAMP, page STRING')
            .alias('data')).select('data.*')
    .withWatermark('event_time', '20 minutes')
    .groupBy(F.window(
            timeColumn='event_time', windowDuration='10 minutes'
    )).count())

write_data_stream = (aggregation_query.writeStream
    .outputMode('update')
    .option('checkpointLocation', get_checkpoint_dir())
    .option('truncate', False)
    .format('console'))

write_data_stream.start().awaitTermination()
