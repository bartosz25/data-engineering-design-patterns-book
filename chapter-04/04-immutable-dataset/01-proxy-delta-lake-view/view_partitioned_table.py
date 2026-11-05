import shutil

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())
    shutil.rmtree('./spark-warehouse', ignore_errors=True)
    def generate_dataframe(prefix: str):
        return spark_session.createDataFrame(
            [(f'{prefix}_1', f'{prefix}_value_1'), (f'{prefix}_2', f'{prefix}_value_2')],
            'id STRING, value STRING').withColumn('partition_timestamp', F.current_timestamp())

    # version 0
    generate_dataframe('first').write.format('delta').partitionBy('partition_timestamp').saveAsTable('partitioned_table')
    spark_session.sql('''
        CREATE OR REPLACE VIEW partitioned_table_most_recent AS
        SELECT * FROM partitioned_table WHERE partition_timestamp = (SELECT MAX(partition_timestamp) FROM partitioned_table) 
    ''')
    print('Most recent version after the 1st run')
    spark_session.sql('SELECT * FROM partitioned_table_most_recent').show(truncate=False)
    # version 1
    generate_dataframe('second').write.format('delta').partitionBy('partition_timestamp').mode('overwrite').saveAsTable('partitioned_table')
    print('Most recent version after the 2nd run')
    spark_session.sql('SELECT * FROM partitioned_table_most_recent').show(truncate=False)
    # version 2
    generate_dataframe('third').write.format('delta').partitionBy('partition_timestamp').mode('overwrite').saveAsTable('partitioned_table')
    print('Most recent version after the 3rd run')
    spark_session.sql('SELECT * FROM partitioned_table_most_recent').show(truncate=False)

    print("Why you can't query the table directly? The overwrite in the partitioned_table table impacts only the given partition!")
    print("Here is what happens when you query the table directly:")
    spark_session.sql('SELECT * FROM partitioned_table').show(truncate=False)

