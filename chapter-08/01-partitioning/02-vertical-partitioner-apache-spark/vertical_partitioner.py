from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import get_base_dir, get_delta_users_table_dir, get_delta_visits_table_dir, get_delta_technical_table_dir

if __name__ == '__main__':
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master('local[*]')
                                                    .config('spark.sql.extensions',
                                                            'io.delta.sql.DeltaSparkSessionExtension')
                                                    .config('spark.sql.catalog.spark_catalog',
                                                            'org.apache.spark.sql.delta.catalog.DeltaCatalog'))
                     .getOrCreate())

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
    input_location = '/tmp/dedp/ch08/01-partitioning/02-vertical-partitioner-apache-spark/input/partition*'
    visits = spark_session.read.schema(visit_schema).json(input_location)

    visits.persist()

    visits_without_user_technical_context = (
        visits
        .drop('user_id')
        .withColumn('context', F.col('context').dropFields('user'))
        .withColumn('context', F.col('context').dropFields('technical'))
    )
    visits_without_user_technical_context.write.format('delta').save(get_delta_visits_table_dir())

    user_context_to_save = visits.selectExpr('visit_id', 'context.user.*', 'user_id').dropDuplicates()
    user_context_to_save.write.format('delta').save(get_delta_users_table_dir())

    technical_context_to_save = visits.selectExpr('visit_id', 'context.technical.*').dropDuplicates()
    technical_context_to_save.write.format('delta').save(get_delta_technical_table_dir())

    visits.unpersist()
