from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_valid_visits_table, get_error_table, get_visit_event_schema

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.catalogImplementation", "hive")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    spark_session.sql(f'''
      SELECT COUNT(*) FROM `default`.`{get_valid_visits_table()}` 
    ''').show()

    spark_session.sql(f'''
      SELECT COUNT(*) FROM `default`.`{get_error_table()}`
    ''').show()
