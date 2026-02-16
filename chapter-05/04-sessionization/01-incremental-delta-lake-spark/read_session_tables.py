from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from config import SESSIONS_DIR, PENDING_SESSIONS_DIR

if __name__ == '__main__':
    spark_session = (configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master('local[*]')
                                                   .config('spark.sql.extensions',
                                                           'io.delta.sql.DeltaSparkSessionExtension')
                                                   .config('spark.sql.catalog.spark_catalog',
                                                           'org.apache.spark.sql.delta.catalog.DeltaCatalog'),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())
    print('PENDING SESSIONS')
    spark_session.read.format('delta').load(PENDING_SESSIONS_DIR).orderBy(F.asc('batch_number')).show(truncate=False)

    print('SESSIONS')
    spark_session.read.format('delta').load(SESSIONS_DIR).orderBy(F.asc('user_id')).show(truncate=False)
