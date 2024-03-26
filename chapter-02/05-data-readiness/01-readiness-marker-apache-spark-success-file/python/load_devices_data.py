from pyspark.sql import SparkSession

from config import DemoConfiguration

if __name__ == "__main__":
    spark_session = SparkSession.builder.master("local[*]").getOrCreate()

    input_dataset = (spark_session.read.schema('type STRING, full_name STRING, version STRING').format('json')
                     .load(DemoConfiguration.INPUT_PATH))

    input_dataset.write.mode('overwrite').format('parquet').save(DemoConfiguration.OUTPUT_PATH)
