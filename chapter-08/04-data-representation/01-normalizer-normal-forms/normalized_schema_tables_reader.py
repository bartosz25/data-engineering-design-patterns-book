from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_normalized_visits_table_dir, \
    get_normalized_visits_context_table_dir, get_normalized_pages_table_dir, get_normalized_pages_categories_table_dir, \
    get_normalized_users_table_dir, get_normalized_ad_table_dir, \
    get_normalized_visits_technical_browser_context_table_dir, get_normalized_visits_technical_device_context_table_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    visits = spark_session.read.format('delta').load(get_normalized_visits_table_dir()).withColumnRenamed('id',
                                                                                                          'visit_id_event')
    visits_context = spark_session.read.format('delta').load(get_normalized_visits_context_table_dir())
    ads = spark_session.read.format('delta').load(get_normalized_ad_table_dir())
    browser = spark_session.read.format('delta').load(get_normalized_visits_technical_browser_context_table_dir())
    device = spark_session.read.format('delta').load(get_normalized_visits_technical_device_context_table_dir())
    pages = spark_session.read.format('delta').load(get_normalized_pages_table_dir())
    pages_categories = spark_session.read.format('delta').load(get_normalized_pages_categories_table_dir())
    users = spark_session.read.format('delta').load(get_normalized_users_table_dir())

    context = (visits_context
               .join(ads, visits_context.ads_id == ads.id, 'left_outer').drop('id')
               .join(browser, visits_context.browsers_id == browser.id, 'left_outer').drop('id')
               .join(device, visits_context.devices_id == device.id, 'left_outer').drop('id'))

    page_with_category = (pages.withColumnRenamed('id', 'page_id').join(pages_categories,
                                                                       pages.page_categories_id == pages_categories.id,
                                                                       'left_outer')
                          .drop('id').withColumnRenamed('page_id', 'id'))

    full_visit = (visits
                  .join(context, visits.visit_id_event == context.visit_id, 'left_outer').drop('visit_id_event')
                  .join(users, visits.users_id == users.id, 'left_outer').drop('id')
                  .join(page_with_category, visits.pages_id == page_with_category.id, 'left_outer').drop('id')
                  .withColumnRenamed('visit_id', 'id')
                  )

    full_visit.show(truncate=False)
