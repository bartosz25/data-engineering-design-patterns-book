from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, IntegerType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("visit_id", LongType(), True),
    StructField("event_time", TimestampType(), False),
    StructField("page", StringType(), False)
])
