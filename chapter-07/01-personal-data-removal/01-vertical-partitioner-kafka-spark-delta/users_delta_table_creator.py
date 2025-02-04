from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, TimestampType

from config import get_delta_users_table_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    (DeltaTable.create(spark_session)
     .location(get_delta_users_table_dir())
     .tableName('users_context')
     .addColumn('user_id', dataType=StringType())
     .addColumn('ip', dataType=StringType())
     .addColumn('login', dataType=StringType())
     .addColumn('connected_since', dataType=TimestampType(), nullable=True)
     .execute())
