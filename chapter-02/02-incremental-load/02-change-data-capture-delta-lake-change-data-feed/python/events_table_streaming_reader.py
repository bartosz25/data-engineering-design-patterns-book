from threading import Thread, Lock

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config(
        "spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    lock = Lock()


    def load_data_to_the_events_table(lock_to_release: Lock):
        partitions = ['date=2023-11-01', 'date=2023-11-02', 'date=2023-11-03',
                      'date=2023-11-04', 'date=2023-11-05', 'date=2023-11-06', 'date=2023-11-07']
        base_dir = '/tmp/dedp/ch02/incremental-load/change-data-capture/input/'
        for partition in partitions:
            print(f'Loading partition {partition}')
            path_to_load = f'{base_dir}/{partition}'
            input_dataset = (
                spark_session.read.schema('visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING')
                .format('json').load(path_to_load))
            if lock_to_release.locked():
                (input_dataset.write.format('delta').saveAsTable('events', overwrite=True))
                print('Releasing lock')
                lock_to_release.release()
            else:
                (input_dataset.write.format('delta').insertInto('events'))


    thread = Thread(target=load_data_to_the_events_table, kwargs={'lock_to_release': lock})
    thread.start()

    lock.acquire(blocking=True)
    while lock.locked():
        pass

    events = (spark_session.readStream.format('delta')
              .option('maxFilesPerTrigger', 4)
              .option('readChangeFeed', 'true')
              .option('startingVersion', 0).table('events'))

    query = events.writeStream.format('console').start()

    query.awaitTermination()
