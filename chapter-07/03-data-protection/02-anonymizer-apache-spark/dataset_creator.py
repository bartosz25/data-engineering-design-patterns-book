from pyspark import Row
from pyspark.sql import SparkSession

from config import get_base_input_dir

if __name__ == '__main__':
    spark = (SparkSession.builder.master('local[*]').getOrCreate())

    spark.createDataFrame(data=[
        Row(user_id=1, birthday='1980-01-20', email='work@contact.com'),
        Row(user_id=2, birthday='1985-01-20', email='dedp@waitingforcode.com'),
        Row(user_id=3, birthday='1990-01-20', email='contact@waitingforcode.com'),
    ]).coalesce(1).write.mode('overwrite').json(get_base_input_dir())

