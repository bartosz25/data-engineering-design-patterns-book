from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType
import pandas as pd
from config import CHECKPOINT_DIR_STREAMING_JOB, PENDING_SESSIONS_DIR, DATA_DIR, SESSIONS_DIR

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                   .master("local[*]")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())

    input_stream_data = spark_session.readStream.format('delta').load(DATA_DIR)

    def generate_sessions(new_visits: DataFrame, batch_number: int):
        result_schema = StructType([StructField('user_id', LongType(), True),
                                    StructField('concatenated_pages', StringType(), True),
                                    StructField('is_completed', BooleanType(), False)])
        def concatenate_visited_pages_by_order(visits: pd.DataFrame, pending_visits: pd.DataFrame) -> pd.DataFrame:
            if visits.shape[0] == 0:
                # session completed
                pending_visits['is_completed'] = True
                pending_visits.rename(columns={'pending_concatenated_pages': 'concatenated_pages'},
                                      inplace=True)
                return pending_visits

            sorted_visits = visits.sort_values(by='visit_time', ascending=True)
            user_id = sorted_visits['user_id'].iloc[0]
            concatenated_pages = " -> ".join(sorted_visits['page'].tolist())
            if pending_visits.shape[0] != 0:
                concatenated_pages += ' -> ' + pending_visits['pending_concatenated_pages'].iloc[0]
            return pd.DataFrame([(user_id, concatenated_pages, False)],
                                columns=['user_id', 'concatenated_pages', 'is_completed'])
        new_visits.persist()
        if batch_number > 0:
            batch_number_for_pending_sessions = batch_number - 1
            pending_sessions = (spark_session.read.format('delta').load(PENDING_SESSIONS_DIR)
                                .filter(f'batch_number = {batch_number_for_pending_sessions}')
                                .withColumnRenamed('concatenated_pages', 'pending_concatenated_pages')
                                .drop('batch_number')
                                .groupBy('user_id'))
            visits_with_maybe_pending_state = new_visits.groupBy('user_id').cogroup(pending_sessions)
        else:
            visits_with_maybe_pending_state = (new_visits.groupBy('user_id')
                                               .cogroup(spark_session.createDataFrame([], 'user_id INT').groupBy('user_id')))
        mapped_sessions = (visits_with_maybe_pending_state
                           .applyInPandas(func=concatenate_visited_pages_by_order, schema=result_schema))

        mapped_sessions.persist()
        (mapped_sessions.filter('is_completed = false')
         .drop('is_completed')
         .withColumn('batch_number', F.lit(batch_number))
         .write.format('delta').mode('append').save(PENDING_SESSIONS_DIR))

        (mapped_sessions.filter('is_completed = true')
         .drop('is_completed')
         .write.format('delta').mode('append').save(SESSIONS_DIR))
        mapped_sessions.unpersist()
        new_visits.unpersist()

    write_query = (input_stream_data.writeStream.foreachBatch(generate_sessions)
                   .trigger(availableNow=True)
                   .option('checkpointLocation', CHECKPOINT_DIR_STREAMING_JOB)
                   .start())

    write_query.awaitTermination()
