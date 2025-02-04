from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_input_visits_dir, get_output_visits_dir, get_output_date_dir, get_output_month_dir, \
    get_output_quarter_dir, get_output_page_dir, get_output_category_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())
    visit_schema = '''
        visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
        context STRUCT<
            referral STRING, ad_id STRING, 
            user STRUCT<
                ip STRING, login STRING, connected_since TIMESTAMP
            >,
            technical STRUCT<
                browser STRING, browser_version STRING, network_type STRING, device_type STRING, device_version STRING
            >
        >
    '''
    visits_dataset = (spark_session.read.schema(visit_schema).format('json')
                      .load(get_input_visits_dir()))
    visits_dataset.cache()

    fact_visit = (visits_dataset.selectExpr(
        'visit_id', 'HASH(page) AS dim_page_id',
        'HASH(TO_DATE(event_time)) AS dim_date_id',
        'DATE_FORMAT(event_time, "HH:mm:ss") AS event_time'
    ))
    fact_visit.write.mode('overwrite').format('delta').save(get_output_visits_dir())

    dim_date = (visits_dataset.selectExpr(
        'HASH(TO_DATE(event_time)) AS date_id',
        'TO_DATE(event_time) AS date',
        'DATE_FORMAT(event_time, "MM    M") AS month_short_name',
        'MONTH(event_time) AS month_number',
        'CONCAT("Quarter ", QUARTER(event_time)) AS quarter_short_name',
        'QUARTER(event_time) AS quarter_number',
        'YEAR(event_time) AS year'
    ).dropDuplicates())
    dim_date.write.mode('overwrite').format('delta').save(get_output_date_dir())

    dim_page = (visits_dataset.selectExpr(
        'HASH(page) AS page_id',
        'page AS page_name',
        'CONCAT("pages/", REPLACE(page, " ", "")) AS page_url',
        'CONCAT("cat_", page) AS category_name',
        'CONCAT("categories/", category_name) AS category_url',
    ).dropDuplicates())
    dim_page.write.mode('overwrite').format('delta').save(get_output_page_dir())
