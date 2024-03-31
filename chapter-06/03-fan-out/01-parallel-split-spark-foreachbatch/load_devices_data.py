from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions

from config import DemoConfiguration

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())
    batch_id = 1
    app_id = 'devices-loader-v1'

    input_dataset = (spark_session.read.schema('type STRING, full_name STRING, version STRING').format('json')
                     .load(DemoConfiguration.INPUT_PATH))
    input_dataset.persist()

    (input_dataset.write.mode('append').format('delta')
     .option('txnVersion', batch_id).option('txnAppId', app_id)
     .save(DemoConfiguration.DEVICES_TABLE))

    raise Exception("Something unexpected has just happened")

    (input_dataset.withColumn('loading_time', functions.current_timestamp())
     .withColumn('full_name', functions.concat_ws(' ', input_dataset.full_name, input_dataset.version))
     .write.option('txnVersion', batch_id).option('txnAppId', app_id)
     .mode('append').format('delta').save(DemoConfiguration.DEVICES_TABLE_ENRICHED))
