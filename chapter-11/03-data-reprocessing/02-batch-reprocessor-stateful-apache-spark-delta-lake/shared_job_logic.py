from pyspark.sql import DataFrame, functions as F

def generate_windowed_stats(input_dataframe: DataFrame) -> DataFrame:
    return input_dataframe.groupBy(
        F.window('event_time', '10 minutes')
    ).count().select(F.to_json(F.struct('window', 'count')).alias('value'))

