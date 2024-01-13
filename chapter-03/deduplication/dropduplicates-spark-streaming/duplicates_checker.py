from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, TimestampType, StringType

from config import get_base_dir

if __name__ == "__main__":
    spark = SparkSession.builder.master('local[*]') \
        .getOrCreate()

    event_schema = StructType([
        StructField("visit_id", StringType()),
        StructField("event_time", TimestampType())
    ])

    visits = (spark.read.text(f'{get_base_dir()}/output/*')
              .select(F.from_json("value", event_schema).alias("value_struct"), "value")
              .selectExpr('value_struct.*'))

    visits.groupBy(['visit_id', 'event_time']).count().filter('count > 1').show()
