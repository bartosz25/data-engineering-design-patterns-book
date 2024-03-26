from pyspark.sql import SparkSession

from config import DemoConfiguration

if __name__ == "__main__":
    spark_session = SparkSession.builder.master("local[*]").getOrCreate()

    input_dataset = spark_session.read.text(DemoConfiguration.INPUT_PATH)

    input_dataset.write.mode('overwrite').text(DemoConfiguration.OUTPUT_PATH)
