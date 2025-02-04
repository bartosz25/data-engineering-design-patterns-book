from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_output_visits_dir, get_output_date_dir, get_output_page_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    fact_visit = spark_session.read.format('delta').load(get_output_visits_dir())
    dim_date = spark_session.read.format('delta').load(get_output_date_dir())
    dim_page = spark_session.read.format('delta').load(get_output_page_dir())

    full_visit = (fact_visit
                  .join(dim_date, fact_visit.dim_date_id == dim_date.date_id, 'left_outer')
                  .join(dim_page, [fact_visit.dim_page_id == dim_page.page_id], 'left_outer'))

    full_visit.show(truncate=True)