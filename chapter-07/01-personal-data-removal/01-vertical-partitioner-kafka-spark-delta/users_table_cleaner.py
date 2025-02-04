from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

from config import get_delta_users_table_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master('local[*]')
                                                    .config('spark.sql.extensions',
                                                            'io.delta.sql.DeltaSparkSessionExtension')
                                                    .config('spark.sql.catalog.spark_catalog',
                                                            'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                                                    ).getOrCreate())

    user_id_to_delete = '140665101097856_0316986e-9e7c-448f-9aac-5727dde96537'
    users_table = DeltaTable.forPath(spark_session, get_delta_users_table_dir())
    users_table.delete(f'user_id = "{user_id_to_delete}"')
