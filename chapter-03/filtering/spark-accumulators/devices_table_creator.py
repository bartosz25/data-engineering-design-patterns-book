import dataclasses
from typing import Iterator, Callable, Any

from delta import configure_spark_with_delta_pip
from pyspark import pandas, Accumulator
from pyspark.sql import SparkSession, DataFrame

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

    spark_context = spark_session.sparkContext

    global_counter = spark_context.accumulator(0)
    @dataclasses.dataclass
    class FilterWithAccumulator:
        name: str
        filter: Callable[[Any], bool]
        accumulator: Accumulator[int]


    filters_with_accumulators = {
        'type': [
            FilterWithAccumulator('type is null', lambda device: device['type'] is not None,
                                  spark_context.accumulator(0)),
            FilterWithAccumulator('type is too short (1 chars or less)', lambda device: len(device['type']) > 1,
                                  spark_context.accumulator(0))
        ],
        'full_name': [
            FilterWithAccumulator('full_name is null', lambda device: device['full_name'] is not None,
                                  spark_context.accumulator(0))
        ],
        'version': [
            FilterWithAccumulator('version is null', lambda device: device['version'] is not None,
                                  spark_context.accumulator(0))
        ]
    }


    def filter_null_type(devices_iterator: Iterator[pandas.DataFrame]):
        def filter_row_with_accumulator(device_row):
            for device_row_attribute in device_row.keys():
                filter_with_accumulator: FilterWithAccumulator
                for filter_with_accumulator in filters_with_accumulators[device_row_attribute]:
                    if not filter_with_accumulator.filter(device_row):
                        filter_with_accumulator.accumulator.add(1)
                        return False
            return True

        for devices_df in devices_iterator:
            yield devices_df[devices_df.apply(lambda device: filter_row_with_accumulator(device), axis=1) == True]


    valid_devices: DataFrame = input_dataset.mapInPandas(filter_null_type, input_dataset.schema)

    valid_devices.write.mode('overwrite').format('delta').save(f'{base_dir}/devices-valid-table')

    for key, accumulators in filters_with_accumulators.items():
        for accumulator_with_filter in accumulators:
            print(f'{key} // {accumulator_with_filter.name} // {accumulator_with_filter.accumulator.value}')
