from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions

from config import get_input_visits_dir, get_one_big_table_dir

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
    visits_dataset = (spark_session.read.schema(visit_schema).format('json').load(get_input_visits_dir())
                      .withColumns({'category_name': functions.expr('CONCAT("cat_", page)'),
                                    'category_url': functions.expr('CONCAT("categories/", category_name)')
    }))
    visits_dataset.write.mode('overwrite').format('delta').save(get_one_big_table_dir())
