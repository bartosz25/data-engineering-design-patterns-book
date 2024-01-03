from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder.master('local[*]') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()

input_data_schema = StructType([
    StructField('payload', StructType([
        StructField('op', StringType()),
        StructField('after', StructType([
            StructField('event_time', LongType()), # NEED to convert as the input date seems not supported (returns dates like |{+143822-02-06 10:39:00, +143822-02-06 10:39:30}|1    |)
        ]))
    ]))
])

input_data_stream = spark.readStream \
    .option('kafka.bootstrap.servers', 'localhost:9094') \
    .option('subscribe', 'dedp.dedp_schema.visits') \
    .option('startingOffsets', 'EARLIEST') \
    .format('kafka').load()

new_visits = input_data_stream.selectExpr('CAST(value AS STRING) AS jsonAsString') \
    .select(F.from_json(F.col('jsonAsString'), input_data_schema).alias('visit')) \
    .filter('visit.payload.op = "c"') \
    .select('visit.payload.after.*') \
    .selectExpr('CAST((event_time / 1000 / 1000) AS TIMESTAMP) AS event_time')

aggregated_counter = new_visits.withWatermark('event_time', '10 seconds').groupBy(
    F.window('event_time', '30 seconds')
).count()

write_data_stream = aggregated_counter.writeStream\
    .format('console').outputMode('update').option('truncate', 'false')

write_data_stream.start().awaitTermination()
