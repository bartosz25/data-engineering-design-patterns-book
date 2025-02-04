from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_input_table_dir, get_output_table_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master('local[*]')
                                                    .config('spark.sql.extensions',
                                                            'io.delta.sql.DeltaSparkSessionExtension')
                                                    .config('spark.sql.catalog.spark_catalog',
                                                            'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                                                    ).getOrCreate())

    (spark_session.read.format('text').load(get_input_table_dir()).write.format('text').mode('overwrite')
     .save(get_output_table_dir()))

