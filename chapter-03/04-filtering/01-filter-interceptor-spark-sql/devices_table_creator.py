from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config import get_base_dir

if __name__ == "__main__":
    base_dir = get_base_dir()

    spark_session = configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension") \
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                                                   ).getOrCreate()

    input_dataset = (spark_session.read.schema('type STRING, full_name STRING, version STRING').format('json')
                     .load(f'{base_dir}/input'))

    input_dataset.createTempView("input")

    spark_session.sql('''
    SELECT * FROM    
    (
        SELECT 
            CASE 
                WHEN (type IS NOT NULL) IS FALSE THEN 'null_type'
                WHEN (LEN(type) > 1) IS FALSE THEN 'short_type'
                WHEN (full_name IS NOT NULL) IS FALSE THEN 'null_full_name'
                WHEN (version IS NOT NULL) IS FALSE THEN 'null_version'
                ELSE NULL
            END AS status_flag,
            type, full_name, version
        FROM input
    )
    ''').createTempView('input_with_flags')

    spark_session.sql('''
    SELECT COUNT(*), status_flag
    FROM input_with_flags
    WHERE status_flag IS NOT NULL
    GROUP BY status_flag
    ''').createTempView('grouped_filters')

    spark_session.sql('SELECT * FROM grouped_filters').show()

    (spark_session.sql('SELECT type, full_name, version FROM input_with_flags WHERE status_flag IS NULL')
     .write.mode('overwrite').format('delta').save(f'{base_dir}/devices-valid-sql-table'))
