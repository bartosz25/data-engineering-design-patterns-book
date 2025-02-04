from pyspark import Row
from pyspark.sql import SparkSession

from config import get_base_input_dir

if __name__ == '__main__':
    spark = (SparkSession.builder.master('local[*]').getOrCreate())

    spark.createDataFrame(data=[
        Row(user_id=1, country='Poland', ssn='0940-0000-1000', salary=50000),
        Row(user_id=2, country='France', ssn='0469-0930-1000', salary=60000),
        Row(user_id=3, country='the USA', ssn='1230-0000-3940', salary=80000),
        Row(user_id=4, country='Spain', ssn='8502-1095-9303', salary=52000),
    ]).coalesce(1).write.mode('overwrite').json(get_base_input_dir())

