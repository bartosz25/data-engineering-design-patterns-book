from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_input_visits_dir, get_output_visits_dir, get_output_date_dir, get_output_month_dir, \
    get_output_quarter_dir, get_output_page_dir, get_output_category_dir, get_output_date_dir_star_schema, \
    get_output_page_dir_star_schema

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    dim_date = spark_session.read.format('delta').load(get_output_date_dir())
    dim_date_month = spark_session.read.format('delta').load(get_output_month_dir())
    dim_date_quarter = spark_session.read.format('delta').load(get_output_quarter_dir())
    dim_page = spark_session.read.format('delta').load(get_output_page_dir())
    dim_page_category = spark_session.read.format('delta').load(get_output_category_dir())

    page_with_category = dim_page.join(dim_page_category,
                                       dim_page.dim_page_category_id == dim_page_category.page_category_id,
                                       'left_outer').dropDuplicates()
    date_with_month_and_quarter = (dim_date
                                   .join(dim_date_month, dim_date.dim_month_id == dim_date_month.month_id,
                                         'left_outer')
                                   .join(dim_date_quarter, dim_date.dim_quarter_id == dim_date_quarter.quarter_id,
                                         'left_outer')).dropDuplicates()

    visits_dataset = (spark_session.read.schema('visit_id STRING, event_time TIMESTAMP,  page STRING').format('json')
                      .load(get_input_visits_dir()))
    fact_visit = (visits_dataset.selectExpr(
        'visit_id',
        'HASH(page) AS dim_page_id',
        'HASH(TO_DATE(event_time)) AS dim_date_id',
        'DATE_FORMAT(event_time, "HH:mm:ss") AS event_time'
    ))
    fact_visit.write.mode('overwrite').format('delta').save(get_output_visits_dir())

    date_with_month_and_quarter.write.mode('overwrite').format('delta').save(get_output_date_dir_star_schema())
    page_with_category.write.mode('overwrite').format('delta').save(get_output_page_dir_star_schema())
