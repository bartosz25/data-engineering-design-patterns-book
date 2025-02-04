from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_input_visits_dir, get_output_visits_dir, get_output_date_dir, get_output_month_dir, \
    get_output_quarter_dir, get_output_page_dir, get_output_category_dir, get_normalized_visits_context_table_dir, \
    get_normalized_pages_table_dir, get_normalized_pages_categories_table_dir, get_normalized_visits_table_dir

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

    visits = (visits_dataset.selectExpr(
        'visit_id',
        'HASH(CONCAT(context.user.ip, context.technical.browser, context.referral)) AS visit_contexts_id',
        'HASH(page) AS pages_id',
        'event_time',
    ))
    visits.write.mode('overwrite').format('delta').save(get_normalized_visits_table_dir())

    visit_context = (visits_dataset.selectExpr(
        'HASH(CONCAT(context.user.ip, context.technical.browser, context.referral)) AS id',
        'context.referral', 'context.ad_id',
        'context.user.ip', 'context.user.login', 'context.user.connected_since',
        'context.technical.browser', 'context.technical.browser_version', 'context.technical.network_type',
        'context.technical.device_type', 'context.technical.device_version'
    ).dropDuplicates())
    visit_context.write.mode('overwrite').format('delta').save(get_normalized_visits_context_table_dir())

    pages = (visits_dataset.selectExpr(
        'HASH(page) AS id',
        'HASH(CONCAT("cat_", page)) AS page_categories_id',
        'page',
    ).dropDuplicates())
    pages.write.mode('overwrite').format('delta').save(get_normalized_pages_table_dir())

    pages_categories = (visits_dataset.selectExpr(
        'HASH(CONCAT("cat_", page)) AS id',
        'CONCAT("cat_", page) AS category_name',
        'CONCAT("categories/", category_name) AS category_url'
    ).dropDuplicates())
    pages_categories.write.mode('overwrite').format('delta').save(get_normalized_pages_categories_table_dir())
