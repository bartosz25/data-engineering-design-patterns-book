from datetime import datetime

from pyspark import Row
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[2]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    while True:
        input_to_write = spark.createDataFrame([
            Row(key='visit_1', value='{"visit_id": "visit_1", "page": "index.html", "user_id": 10, "event_time": '+ datetime.utcnow().isoformat() + '}'),
            Row(key='visit_2', value='{"visit_id": "visit_2", "page": "main.html", "user_id": 12, "event_time": '+ datetime.utcnow().isoformat() + '}'),
            Row(key='visit_3', value='{"visit_id": "visit_3", "page": "index.html", "user_id": 14, "event_time": ' + datetime.utcnow().isoformat() + '}'),
            Row(key='visit_4', value='{"visit_id": "visit_4", "page": "contact.html", "user_id": 16, "event_time": ' + datetime.utcnow().isoformat() + '}')
        ])
        
        (input_to_write.write.format('kafka')
         .option('kafka.bootstrap.servers', 'localhost:9094')
         .option('topic', 'visits')
         .save())
