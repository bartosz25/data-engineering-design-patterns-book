from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_input_visits_dir, get_normalized_visits_context_table_dir, \
    get_normalized_pages_table_dir, get_normalized_pages_categories_table_dir, get_normalized_visits_table_dir, \
    get_normalized_visits_technical_browser_context_table_dir, \
    get_normalized_visits_technical_device_context_table_dir, get_normalized_ad_table_dir, \
    get_normalized_users_table_dir

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
        'visit_id AS id',
        'HASH(page) AS pages_id',
        'user_id AS users_id',
        'event_time',
    ))
    visits.write.mode('overwrite').format('delta').save(get_normalized_visits_table_dir())

    users = (visits_dataset.selectExpr(
        'user_id AS id',
        'CONCAT("user", user_id) AS login'
    ))
    users.write.mode('overwrite').format('delta').save(get_normalized_users_table_dir())

    visit_context = (visits_dataset.selectExpr(
        'visit_id',
        'context.ad_id AS ads_id',
        'HASH(CONCAT(context.technical.browser, context.technical.browser_version)) AS browsers_id',
        'HASH(CONCAT(context.technical.device_type, context.technical.device_version)) AS devices_id',
        'context.referral',
        'context.user.ip AS user_ip',
        'context.user.connected_since AS user_connected_since'
    ).dropDuplicates())
    visit_context.write.mode('overwrite').format('delta').save(get_normalized_visits_context_table_dir())

    ads = (visits_dataset.selectExpr(
        'context.ad_id AS id',
        'CONCAT("ad#", context.ad_id) AS name'
    ).dropDuplicates())
    ads.write.mode('overwrite').format('delta').save(get_normalized_ad_table_dir())

    technical_browser_context = (visits_dataset.selectExpr(
        'HASH(CONCAT(context.technical.browser, context.technical.browser_version)) AS id',
        'context.technical.browser AS name', 'context.technical.browser_version AS version'
    ).dropDuplicates())
    (technical_browser_context.write.mode('overwrite').format('delta')
     .save(get_normalized_visits_technical_browser_context_table_dir()))

    technical_device_context = (visits_dataset.selectExpr(
        'HASH(CONCAT(context.technical.device_type, context.technical.device_version)) AS id',
        'context.technical.device_type AS type', 'context.technical.device_version AS version'
    ).dropDuplicates())
    technical_device_context.write.mode('overwrite').format('delta').save(get_normalized_visits_technical_device_context_table_dir())

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
