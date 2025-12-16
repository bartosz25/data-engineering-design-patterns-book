from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType

if __name__ == "__main__":
    spark = (SparkSession.builder.master('local[*]')
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0')
        .getOrCreate())
    
    input_directory = "/tmp/dedp/ch11/01-data-ingestion/01-api-gateway-django/fallback/topic=visits/*"
    input_data_schema = StructType([
        StructField("key", StringType()), StructField("value", StringType()),
        StructField("referral", StringType())
    ])
    value_json_schema = StructType([
        StructField("event_time", LongType())
    ])
    
    input_data = (spark.readStream
        .schema(input_data_schema) 
        .option("latestFirst", False)
        .option("maxFilesPerTrigger", 100)
        .json(input_directory) 
        .withColumn('visit_data', F.from_json("value", value_json_schema))
        .select(
            'key', 'referral',
            F.col('visit_data.event_time').alias('visit_time'), 'value')
    )
    
    input_data_repartitioned_by_key = input_data.repartition(20, 'key')
    
    
    def write_sorted_data_to_kafka(dataset_repartitioned_by_key: DataFrame, batch_number: int):
        input_data_sorted_events = dataset_repartitioned_by_key.sortWithinPartitions('key', 'visit_time')
        input_data_sorted_events_with_header = (input_data_sorted_events.withColumn('headers', F.array(
            F.struct(F.lit('source').alias('key'), F.lit('replay'.encode('UTF-8')).alias('value')),
            F.struct(F.lit('referral').alias('key'), F.encode('referral', 'UTF-8').alias('value'))
        ))).drop('referral')

        (input_data_sorted_events_with_header
            .write 
            .format('kafka')
            .option('kafka.bootstrap.servers', 'localhost:9094')
            .option('topic', 'visits')
            .save())
    
    
    write_data_stream = (input_data_repartitioned_by_key
        .writeStream 
        .option('checkpointLocation', '/tmp/dedp/ch11/01-data-ingestion/01-api-gateway-django/checkpoint')
        .trigger(availableNow=True) 
        .foreachBatch(write_sorted_data_to_kafka))
    
    write_data_stream.start().awaitTermination()
