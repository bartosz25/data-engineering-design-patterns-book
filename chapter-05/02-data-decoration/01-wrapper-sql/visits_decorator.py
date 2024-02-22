from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import DemoConfiguration

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

    input_dataset = (spark_session.read.schema(visit_schema).format('json')
                     .load(DemoConfiguration.INPUT_PATH))
    input_dataset.cache()
    input_dataset.createOrReplaceTempView('input_visits')

    visits_struct_decorated = spark_session.sql('''
        SELECT *, 
            named_struct(
                'is_connected', 
                CASE WHEN context.user.connected_since IS NULL 
                    THEN false 
                    ELSE true END,
                'page_referral_key',
                CONCAT_WS('-', page, context.referral)
            ) AS decorated
         FROM input_visits
    ''')
    visits_struct_decorated.write.format('delta').mode('overwrite').save(DemoConfiguration.TABLE_DECORATED_STRUCT)

    visits_raw_decorated = spark_session.sql('''
        SELECT
            CASE WHEN context.user.connected_since IS NULL 
                THEN false 
                ELSE true END AS is_connected,
            CONCAT_WS('-', page, context.referral) AS page_referral_key,
            STRUCT(visit_id, event_time, user_id, page, context) AS raw
        FROM input_visits
    ''')
    visits_raw_decorated.write.format('delta').mode('overwrite').save(DemoConfiguration.TABLE_RAW_STRUCT)

    visits_flattened = spark_session.sql('''
        SELECT *,
            CASE WHEN context.user.connected_since IS NULL 
                THEN false 
                ELSE true END AS is_connected,
            CONCAT_WS('-', page, context.referral) AS page_referral_key
        FROM input_visits
    ''')

    visits_flattened.write.format('delta').mode('overwrite').save(DemoConfiguration.TABLE_FLATTENED)
