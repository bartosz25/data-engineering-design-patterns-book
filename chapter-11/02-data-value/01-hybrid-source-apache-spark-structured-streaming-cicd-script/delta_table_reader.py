from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    input_dataset = (spark_session.read.format('delta')
     .load('/tmp/dedp/ch11/11-streaming/02-hybrid-source-apache-spark-structured-streaming-cicd-script/delta_table'))
    input_dataset.sort(F.col('event_time').asc()).show(n=50, truncate=False)
