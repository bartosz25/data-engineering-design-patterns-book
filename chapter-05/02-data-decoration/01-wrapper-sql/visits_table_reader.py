from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

from config import DemoConfiguration

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    print('========= STRUCT DECORATED =========')
    spark_session.read.format('delta').load(DemoConfiguration.TABLE_DECORATED_STRUCT).show(truncate=False, n=1)

    print('========= STRUCT RAW =========')
    spark_session.read.format('delta').load(DemoConfiguration.TABLE_RAW_STRUCT).show(truncate=False, n=1)

    print('========= FLATTEN =========')
    spark_session.read.format('delta').load(DemoConfiguration.TABLE_FLATTENED).show(truncate=False, n=1)
