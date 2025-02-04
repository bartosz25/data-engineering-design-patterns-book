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
        visit_id STRING, 
        event_time TIMESTAMP, 
        user_id STRING, 
        page STRING
    ) USING delta
    ''')
    spark_session.sql('DESCRIBE DETAIL default.visits').select('name', 'tableFeatures').show(truncate=False)
    spark_session.sql('SHOW TBLPROPERTIES default.visits').show(truncate=False)

    spark_session.sql('''
    INSERT INTO default.visits (visit_id, event_time, user_id, page) VALUES 
    ("visit 1", TIMESTAMP "2024-07-01T10:00:00.840Z", "user 1", "index.html")
    ''')

    spark_session.sql("SELECT * FROM default.visits").show(truncate=False)

    try:
        spark_session.sql('''
        INSERT INTO default.visits (visit_id, event_time, user_id, page) VALUES 
            (NULL, TIMESTAMP "2023-07-01T10:00:00.840Z", "user 2", "index.html")
        ''')
    except Exception as e:
        print(e)

    try:
        spark_session.sql('''
        INSERT INTO default.visits (visit_id, event_time, user_id, page) VALUES 
            ("visit 2 from the future", TIMESTAMP "2036-07-01T10:00:00.840Z", "user 2 from the future", "index.html")
        ''')
    except Exception as e:
        print(e)

    spark_session.sql("SELECT * FROM default.visits").show(truncate=False)

    try:
        spark_session.sql('''
        INSERT INTO default.visits (visit_id, event_time, user_id, page) VALUES 
            ("visit 1", TIMESTAMP "2024-07-01T10:00:00.840Z", "user 1", "index.html"),
            (NULL, TIMESTAMP "2023-07-01T10:00:00.840Z", "user 2", "index.html")
        ''')
    except Exception as e:
        print(e)

    spark_session.sql("SELECT * FROM default.visits").show(truncate=False)