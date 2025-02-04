import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.catalogImplementation", "hive")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    spark_session.sql('DROP TABLE IF EXISTS default.visits')
    spark_session.sql('''
    CREATE TABLE default.visits (
        visit_id STRING NOT NULL, 
        event_time TIMESTAMP NOT NULL, 
        user_id STRING NOT NULL, 
        page STRING NOT NULL
    ) USING delta  
    ''')
    spark_session.sql('DESCRIBE DETAIL default.visits').select('name', 'tableFeatures').show(truncate=False)
    spark_session.sql('SHOW TBLPROPERTIES default.visits').show(truncate=False)

