from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_delta_users_table_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master('local[*]')
                                                    .config('spark.sql.extensions',
                                                            'io.delta.sql.DeltaSparkSessionExtension')
                                                    .config('spark.sql.catalog.spark_catalog',
                                                            'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                                                    ).getOrCreate())

    (spark_session.read.format('delta').load(get_delta_users_table_dir()).
     sort('user_id').show(truncate=False))

