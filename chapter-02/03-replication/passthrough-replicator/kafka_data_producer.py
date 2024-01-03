from pyspark import Row
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    dataset = spark.createDataFrame(data=[
        Row(key='event1', value='1-1', headers=[Row(key='h1', value='header1'.encode('utf-8')),
                                                Row(key='h2', value='header2'.encode('utf-8'))]),
        Row(key='event2', value='2-1', headers=[Row(key='h3', value='header3'.encode('utf-8')),
                                                Row(key='h4', value='header4'.encode('utf-8'))]),
        Row(key='event3', value='3-1', headers=[Row(key='h5', value='header5'.encode('utf-8')),
                                                Row(key='h6', value='header6'.encode('utf-8'))]),
        Row(key='event1', value='1-2', headers=[Row(key='h1', value='header1'.encode('utf-8')),
                                                Row(key='h2', value='header2'.encode('utf-8'))])
    ]).repartition(1)

    (dataset.sortWithinPartitions('key', ascending=True).write.format('kafka')
     .option('kafka.bootstrap.servers', 'localhost:9094')
     .option('topic', 'events').option('includeHeaders', 'true').save())
