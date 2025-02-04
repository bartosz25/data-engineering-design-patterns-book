from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_delta_users_table_dir, get_delta_technical_table_dir, get_delta_visits_table_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master('local[*]')
                                                    .config('spark.sql.extensions',
                                                            'io.delta.sql.DeltaSparkSessionExtension')
                                                    .config('spark.sql.catalog.spark_catalog',
                                                            'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                                                    ).getOrCreate())

    print('⌛ Reading users')
    (spark_session.read.format('delta').load(get_delta_users_table_dir()).
     sort('visit_id').show(truncate=False, n=3))

    print('⌛ Reading technical')
    (spark_session.read.format('delta').load(get_delta_technical_table_dir()).
     sort('visit_id').show(truncate=False, n=3))

    print('⌛ Reading visits')
    (spark_session.read.format('delta').load(get_delta_visits_table_dir()).
     sort('visit_id').show(truncate=False, n=3))

