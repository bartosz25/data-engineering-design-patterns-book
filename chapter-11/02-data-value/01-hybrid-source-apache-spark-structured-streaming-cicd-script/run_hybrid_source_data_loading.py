from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from historical_data_loader import load_historical_data
from real_time_data_loader import load_real_time_data

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                                    extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                    ).getOrCreate())


    load_historical_data(spark_session)
    load_real_time_data(spark_session)
