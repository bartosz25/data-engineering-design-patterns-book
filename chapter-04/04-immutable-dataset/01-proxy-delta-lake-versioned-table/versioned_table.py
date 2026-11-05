import shutil

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())
    shutil.rmtree('./spark-warehouse', ignore_errors=True)
    def generate_dataframe(prefix: str):
        return spark_session.createDataFrame(
            [(f'{prefix}_1', f'{prefix}_value_1'), (f'{prefix}_2', f'{prefix}_value_2')],
            'id STRING, value STRING')

    spark_session.sql('DROP TABLE IF EXISTS versioned_table')

    # version 0
    generate_dataframe('first').write.format('delta').saveAsTable('versioned_table')
    # version 1
    generate_dataframe('second').write.format('delta').mode('overwrite').saveAsTable('versioned_table')
    # version 2
    generate_dataframe('third').write.format('delta').mode('overwrite').saveAsTable('versioned_table')

    print('Most recent version')
    spark_session.sql('SELECT * FROM versioned_table').show(truncate=False)

    print('Historical version (2nd generated DataFrame)')
    spark_session.sql('SELECT * FROM versioned_table VERSION AS OF 1').show(truncate=False)