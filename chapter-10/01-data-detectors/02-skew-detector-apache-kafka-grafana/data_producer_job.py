from datetime import datetime

from pyspark import Row
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[2]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    for i in range(0, 6):
        input_to_write = spark.createDataFrame([
            Row(key='visit_1', value='{"visit_id": "visit_1", "page": "index.html", "user_id": 10, "event_time": '+ datetime.utcnow().isoformat() + '}',
                partition=0),
            Row(key='visit_2', value='{"visit_id": "visit_2", "page": "contact.html", "user_id": 10, "event_time": '+ datetime.utcnow().isoformat() + '}',
                partition=0),
            Row(key='visit_3', value='{"visit_id": "visit_3", "page": "index.html", "user_id": 10, "event_time": '+ datetime.utcnow().isoformat() + '}',
                partition=0),
            Row(key='visit_4', value='{"visit_id": "visit_4", "page": "categories.html", "user_id": 10, "event_time": '+ datetime.utcnow().isoformat() + '}',
                partition=0),
            Row(key='visit_5', value='{"visit_id": "visit_5", "page": "main.html", "user_id": 12, "event_time": '+ datetime.utcnow().isoformat() + '}',
                partition=0),
            Row(key='visit_6', value='{"visit_id": "visit_6", "page": "index.html", "user_id": 14, "event_time": ' + datetime.utcnow().isoformat() + '}',
                partition=1),
            Row(key='visit_7', value='{"visit_id": "visit_7", "page": "contact.html", "user_id": 16, "event_time": ' + datetime.utcnow().isoformat() + '}',
                partition=1)
        ])

        (input_to_write.selectExpr('key', 'value', 'CAST(partition AS INT)').write.format('kafka')
         .option('kafka.bootstrap.servers', 'localhost:9094')
         .option('topic', 'visits')
         .save())
