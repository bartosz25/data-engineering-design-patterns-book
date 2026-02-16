import datetime
from typing import Iterable, Any, List, Dict, Tuple

import pandas
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, ArrayType

from config import CHECKPOINT_DIR_STREAMING_JOB, DATA_DIR, SESSIONS_DIR

if __name__ == "__main__":
    spark_session = (configure_spark_with_delta_pip(spark_session_builder=SparkSession.builder
                                                    .master("local[*]")
                                                    .config('spark.sql.shuffle.partitions', 2)
                                                    .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                   ).getOrCreate())

    input_stream_data = spark_session.readStream.format('delta').load(DATA_DIR)


    def map_visits(user_id_tuple: Any, input_rows: Iterable[pandas.DataFrame],
                   current_state: GroupState) -> Iterable[pandas.DataFrame]:
        def get_session_to_return(visited_pages: List[Tuple[str, str]]) -> Dict[str, Any]:
            print(visited_pages)
            sorted_visits = sorted(visited_pages,
                                   key=lambda event_time_with_page: event_time_with_page.visit_time_as_milliseconds)
            first_visit_time_as_milliseconds = sorted_visits[0].visit_time_as_milliseconds
            last_visit_time_as_milliseconds = sorted_visits[len(sorted_visits) - 1].visit_time_as_milliseconds

            duration_in_milliseconds = last_visit_time_as_milliseconds - first_visit_time_as_milliseconds
            # Using the [...] is required to avoid
            # "ValueError: If using all scalar values, you must pass an index" error
            return {
                "start_time": [datetime.datetime.fromtimestamp(first_visit_time_as_milliseconds / 1000.0,
                                                               tz=datetime.timezone.utc)],
                "end_time": [datetime.datetime.fromtimestamp(last_visit_time_as_milliseconds / 1000.0,
                                                             tz=datetime.timezone.utc)],
                "visited_pages": [sorted_visits],
                "duration_in_milliseconds": [duration_in_milliseconds],
                "user_id": [user_id],
            }

        session_expiration_time_40min_as_ms = 40 * 60 * 1000
        user_id = user_id_tuple[0]
        visit_to_return = None
        if current_state.hasTimedOut:
            print(f"Session ({current_state.get}) expired for {user_id}; let's generate the final output here")
            visits, = current_state.get
            visit_to_return = get_session_to_return(visits)
            current_state.remove()
        else:
            should_use_event_time_for_watermark = current_state.getCurrentWatermarkMs() == 0
            base_watermark = current_state.getCurrentWatermarkMs()
            new_visits = []
            for input_df_for_group in input_rows:
                input_df_for_group['visit_time_as_milliseconds'] = (input_df_for_group['visit_time']
                    .apply(lambda x: int(pandas.Timestamp(x).timestamp()) * 1000))
                if should_use_event_time_for_watermark:
                    base_watermark = int(input_df_for_group['visit_time_as_milliseconds'].max())

                new_visits = input_df_for_group[['visit_time_as_milliseconds', 'page']].to_dict(orient='records')

            visits_so_far = []
            if current_state.exists:
                visits_so_far, = current_state.get

            print(f'visits_so_far={visits_so_far}')
            visits_for_state = visits_so_far + new_visits
            print(f'Updating {visits_for_state}')
            current_state.update((visits_for_state,))

            timeout_timestamp = base_watermark + session_expiration_time_40min_as_ms
            current_state.setTimeoutTimestamp(timeout_timestamp)

        if visit_to_return:
            print(visit_to_return)
            yield pandas.DataFrame(visit_to_return)


    grouped_visits = (input_stream_data.withWatermark('visit_time', '15 minutes')
     .groupBy(F.col('user_id')))
    visited_pages_type = ArrayType(StructType([
        StructField("page", StringType()),
        StructField("visit_time_as_milliseconds", LongType())
    ]))
    visits_state = grouped_visits.applyInPandasWithState(
        func=map_visits,
        outputStructType=StructType([
            StructField("start_time", TimestampType()),
            StructField("end_time", TimestampType()),
            StructField("visited_pages", visited_pages_type),
            StructField("duration_in_milliseconds", LongType()),
            StructField("user_id", LongType())
        ]),
        stateStructType=StructType([
            StructField("visits", visited_pages_type),
        ]),
        outputMode="append",
        timeoutConf="EventTimeTimeout"
    )

    write_query = (visits_state.writeStream.outputMode('append')
                   .trigger(availableNow=True)
                   .option('checkpointLocation', CHECKPOINT_DIR_STREAMING_JOB)
                   .format('delta')
                   .start(path=SESSIONS_DIR))

    write_query.awaitTermination()
