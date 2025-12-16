import datetime
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, GroupedData
from pyspark.sql.types import IntegerType, StructType, StructField, BooleanType, StringType

from config import VISITS_TABLE, OUTPUT_TOPIC_NAME
from reprocessing_data_preparator import ReprocessingDataPreparator
from visit_duration_stateful_processor import VisitsDurationCalculator

if __name__ == "__main__":
    #start_window = '2025-09-04T10:00:00.000'
    #end_window = '2025-09-04T11:00:00.000'
    #reprocessing_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    start_window = sys.argv[1]
    end_window = sys.argv[2]
    reprocessing_id = sys.argv[3]
    print(f'''
    ******* Reprocessing for {start_window} - {end_window}
    ******* Reprocessing id: {reprocessing_id}
    ''')

    spark_session = (configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master('local[*]').enableHiveSupport()
                                                   .config('spark.sql.shuffle.partitions', 2)
                                                   .config('spark.sql.extensions',
                                                           'io.delta.sql.DeltaSparkSessionExtension')
                                                   .config('spark.sql.catalog.spark_catalog',
                                                           'org.apache.spark.sql.delta.catalog.DeltaCatalog'),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())

    reprocessing_data_preparator = ReprocessingDataPreparator(
        start_time=start_window, end_time=end_window, reprocessing_id=reprocessing_id
    )
    reprocessing_data_preparator.prepare_datasets_for_reprocessing(spark_session=spark_session)

    adjacent_data_to_reprocess = spark_session.read.format('delta').table(reprocessing_data_preparator.visits_to_reprocess_table)
    complete_data_to_reprocess = adjacent_data_to_reprocess.unionByName(
        spark_session.sql(f'SELECT * FROM {VISITS_TABLE} WHERE event_time >= "{start_window}" AND event_time < "{end_window}" ')
    )
    # complete_data_to_reprocess.orderBy(F.col('event_time').asc()).show(truncate=False, n=50)

    data_to_reprocess = complete_data_to_reprocess

    reprocessed_sessions = data_to_reprocess.groupBy('user_id').transformWithStateInPandas(
        statefulProcessor=VisitsDurationCalculator(is_reprocessing=True),
        outputStructType=VisitsDurationCalculator.OUTPUT_SCHEMA,
        timeMode='eventTime',
        outputMode='update',
    ).selectExpr(
        'TO_JSON(STRUCT(*)) AS value',
        f'''ARRAY(
                STRUCT("source" AS key, CAST("reprocessing" AS BINARY) AS value),
                STRUCT("run_id" AS key, CAST("{reprocessing_id}" AS BINARY) AS value)
            ) AS headers
        '''
    )

    write_query = (reprocessed_sessions.write.format('kafka')
                   .option('topic', OUTPUT_TOPIC_NAME)
                   .option('kafka.bootstrap.servers', 'localhost:9094')
                   .save())
