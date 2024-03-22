from enum import Enum

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame


class OutputType(str, Enum):
    delta_lake = 'delta'
    csv = 'csv'


class OutputGenerationFactory:

    def __init__(self, output_type: OutputType):
        self.type = output_type

    def get_spark_session(self) -> SparkSession:
        if self.type == OutputType.delta_lake:
            return (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                   ).getOrCreate())
        else:
            return SparkSession.builder.master("local[*]").getOrCreate()

    def write_devices_data(self, devices_data: DataFrame, output_location: str):
        if self.type == OutputType.delta_lake:
            devices_data.write.mode('append').format('delta').save(output_location)
        else:
            devices_data.coalesce(1).write.mode('append').format('csv').save(output_location)
