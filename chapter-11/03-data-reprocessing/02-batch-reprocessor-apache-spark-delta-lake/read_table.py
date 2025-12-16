from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from config import DATA_DIR

if __name__ == "__main__":
    spark_session =(configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                   ).getOrCreate())

    spark_session.read.format('delta').load(DATA_DIR).orderBy(
        F.asc('partition'), F.asc('offset')
    ).show(truncate=False, n=1000)
