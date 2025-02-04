from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_output_visits_dir, get_output_date_dir, get_output_page_dir, get_one_big_table_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    visits_table = spark_session.read.format('delta').load(get_one_big_table_dir())

    visits_table.show(truncate=False)
