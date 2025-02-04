from delta import configure_spark_with_delta_pip
from pyspark import Row
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.catalogImplementation", "hive")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())
    events = spark_session.createDataFrame(data=[
        Row(visit_id='event1', page='index.html', event_time=123),
        Row(visit_id='event2', page='contact.html', event_time=456),
        Row(visit_id='event3', page='about.html', event_time=789),
    ])
    events.write.mode('overwrite').format('delta').saveAsTable('visits')

    new_events_old_schema = spark_session.createDataFrame(data=[
        Row(visit_id='event4', page='index.html', event_time=91011),
    ])
    new_events_old_schema.write.format('delta').insertInto('visits')

    spark_session.read.table('visits').show(truncate=False)

    new_events_new_schema = spark_session.createDataFrame(data=[
        Row(visit_id='event4', page='about.html', event_time='2024-05-05T09:00:00'),
    ])
    new_events_new_schema.write.format('delta').insertInto('visits')

    spark_session.read.table('visits').show(truncate=False)
    spark_session.read.table('visits').printSchema()