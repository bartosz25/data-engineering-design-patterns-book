from datetime import datetime

from delta import configure_spark_with_delta_pip
from pyspark import Row
from pyspark.sql import SparkSession, functions as F

from config import VISITS_TABLE
from schema import EVENT_SCHEMA

if __name__ == "__main__":
    spark_session =(configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master('local[*]').enableHiveSupport()
                                                   .config('spark.sql.shuffle.partitions', 2)
                                                   .config('spark.sql.extensions',
                                                           'io.delta.sql.DeltaSparkSessionExtension')
                                                   .config('spark.sql.catalog.spark_catalog',
                                                           'org.apache.spark.sql.delta.catalog.DeltaCatalog'),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())
    page_counter = 0
    event_id = 0
    def visit(user_id: int, hour: int, minutes: int) -> Row:
        global page_counter, event_id
        page_counter += 1
        event_id += 1
        return Row(event_id=event_id, user_id=user_id, event_time=datetime(2025, 9, 4, hour, minutes), page=f'page{page_counter}')

    dataset_to_write = spark_session.createDataFrame(
        data=[
            # 8:00
            visit(user_id=4, hour=8, minutes=20), visit(user_id=4, hour=8, minutes=21),
            visit(user_id=4, hour=8, minutes=22),
            visit(user_id=1, hour=8, minutes=26), visit(user_id=1, hour=8, minutes=29),
            visit(user_id=1, hour=8, minutes=47), visit(user_id=1, hour=8, minutes=59),
            # 9:00
            visit(user_id=1, hour=9, minutes=6), visit(user_id=1, hour=9, minutes=15),
            visit(user_id=1, hour=9, minutes=21), visit(user_id=1, hour=9, minutes=30),
            visit(user_id=1, hour=9, minutes=35), visit(user_id=1, hour=9, minutes=40),
            visit(user_id=1, hour=9, minutes=49), visit(user_id=1, hour=9, minutes=50),
            visit(user_id=1, hour=9, minutes=51), visit(user_id=1, hour=9, minutes=52),
            visit(user_id=2, hour=9, minutes=49),
            # 10:00
            visit(user_id=1, hour=10, minutes=0), visit(user_id=1, hour=10, minutes=10),
            visit(user_id=1, hour=10, minutes=20), visit(user_id=1, hour=10, minutes=30),
            visit(user_id=1, hour=10, minutes=40), visit(user_id=1, hour=10, minutes=50),
            visit(user_id=2, hour=10, minutes=0), visit(user_id=2, hour=10, minutes=5), # user 2, session 1
            visit(user_id=2, hour=10, minutes=45), visit(user_id=2, hour=10, minutes=50), # user 2, session 2
            visit(user_id=3, hour=10, minutes=52),
            # 11:00
            visit(user_id=1, hour=11, minutes=0), visit(user_id=1, hour=11, minutes=25),
            visit(user_id=1, hour=11, minutes=40), visit(user_id=1, hour=11, minutes=50),
            visit(user_id=1, hour=11, minutes=52), visit(user_id=1, hour=11, minutes=53),
            visit(user_id=3, hour=11, minutes=2), visit(user_id=3, hour=11, minutes=12),
            visit(user_id=3, hour=11, minutes=22), visit(user_id=3, hour=11, minutes=32),
            visit(user_id=3, hour=11, minutes=42), visit(user_id=3, hour=11, minutes=52),
            # 12:00
            visit(user_id=1, hour=12, minutes=0),
            visit(user_id=6, hour=12, minutes=0),
            visit(user_id=7, hour=12, minutes=25)
        ], schema=EVENT_SCHEMA
    )
    (dataset_to_write
     .withColumn('year', F.year(F.col('event_time')))
     .withColumn('month', F.month(F.col('event_time')))
     .withColumn('day', F.dayofmonth(F.col('event_time')))
     .withColumn('hour', F.hour(F.col('event_time')))
     .write
     .partitionBy('year', 'month', 'day', 'hour')
     .format('delta').mode('overwrite').saveAsTable(VISITS_TABLE))