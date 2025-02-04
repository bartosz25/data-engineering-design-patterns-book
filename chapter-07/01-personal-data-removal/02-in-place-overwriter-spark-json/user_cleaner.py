import os
import shutil
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from config import get_input_table_dir, get_output_table_dir, get_staging_table_dir

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    input_raw_data = spark_session.read.text(get_input_table_dir())

    input_with_user_column = input_raw_data.withColumn(
        'user', F.from_json('value', 'user_id STRING')
    )

    user_to_delete = '139621130423168_029fba78-15dc-4944-9f65-00636566f75b'
    rows_to_save = input_with_user_column.filter(f'user.user_id != "{user_to_delete}"').select('value')
    rows_to_save.write.mode('overwrite').format('text').save(get_staging_table_dir())

    # only once all writes succeeded, replace the staging by the output dir
    shutil.rmtree(get_output_table_dir())
    os.rename(
        get_staging_table_dir(),
        get_output_table_dir()
    )
