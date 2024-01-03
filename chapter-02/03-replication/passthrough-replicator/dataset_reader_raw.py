from pyspark.sql import SparkSession

from config import DemoConfiguration

if __name__ == "__main__":
    spark_session = SparkSession.builder.master("local[*]").getOrCreate()

    # We don't specify the schema on purpose; the dataset is small enough, we can let Spark deduce it
    # Generally leaving the schema empty for semi-structured datasets is not a good idea since it's error-prone
    input_dataset = (spark_session.read.format('json')
                     .load(DemoConfiguration.OUTPUT_PATH))

    input_dataset.show(truncate=False, n=4)
