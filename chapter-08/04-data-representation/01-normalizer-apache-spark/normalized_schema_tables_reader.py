from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_normalized_visits_table_dir, \
    get_normalized_visits_context_table_dir, get_normalized_pages_table_dir, get_normalized_pages_categories_table_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    visits = spark_session.read.format('delta').load(get_normalized_visits_table_dir())
    visits_context = spark_session.read.format('delta').load(get_normalized_visits_context_table_dir())
    pages = spark_session.read.format('delta').load(get_normalized_pages_table_dir())
    pages_categories = spark_session.read.format('delta').load(get_normalized_pages_categories_table_dir())

    page_with_category = pages.join(pages_categories,
                                    pages.page_categories_id == pages_categories.id,
                                    'left_outer')

    full_visit = (visits
                  .join(visits_context, visits.visit_contexts_id == visits_context.id, 'left_outer').drop('id')
                  .join(page_with_category, visits.pages_id == pages.id, 'left_outer').drop('id')
                  )

    full_visit.show(truncate=False)
