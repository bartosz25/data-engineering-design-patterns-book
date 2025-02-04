from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_table_zordered_dir, get_visit_id

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())
    visits_table_zorder = spark_session.read.format('delta').load(get_table_zordered_dir())

    (visits_table_zorder
     .filter(f'visit_id = "{get_visit_id()}" OR page = "categories"')
     .write.format('noop').mode('overwrite').save())

    # To keep the UI alive
    while True:
        pass