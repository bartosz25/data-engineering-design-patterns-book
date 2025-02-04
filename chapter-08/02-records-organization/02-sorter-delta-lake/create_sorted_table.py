from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

from config import get_input_dir, get_table_zordered_dir, get_table_flat_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                        .config("spark.sql.catalogImplementation", "hive")
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
                     .load(get_input_dir()))

    spark_session.conf.set("spark.sql.files.maxRecordsPerFile", 10000)
    (input_dataset.write.mode('overwrite').format('delta')
     .saveAsTable('devices'))

    DeltaTable.forPath(spark_session, get_table_zordered_dir()).optimize().executeZOrderBy(['visit_id', 'page'])
    (input_dataset.write.mode('overwrite').format('delta')
     .save(get_table_flat_dir()))
