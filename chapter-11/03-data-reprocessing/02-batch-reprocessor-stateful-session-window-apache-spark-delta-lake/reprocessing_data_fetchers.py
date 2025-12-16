import datetime

from pyspark.sql import DataFrame, SparkSession, functions as F

from config import VISITS_TABLE


def get_users_to_reprocess(spark_session: SparkSession, start_time: str, end_time: str) -> DataFrame:
    users_to_reprocess_df = spark_session.sql(f'''
        SELECT MIN(event_time) AS min_event_time, MAX(event_time) AS max_event_time, user_id 
        FROM {VISITS_TABLE} 
        WHERE event_time >= "{start_time}" AND event_time <= "{end_time}" 
        GROUP BY user_id
    ''')
    return users_to_reprocess_df



def get_data_to_reprocess_from_past_partitions(spark_session: SparkSession, users_to_reprocess_table: str,
                                               current_partition: str, output_table_name: str,
                                               max_iterations: int = 100):
    date_format = '%Y-%m-%dT%H:%M:%S.%f'
    timestamp_partition_start = datetime.datetime.strptime(current_partition, date_format)
    timestamp_partition_end = datetime.datetime.strptime(current_partition, date_format) - datetime.timedelta(hours=1)
    current_iteration = 1
    users_to_reprocess_df = spark_session.read.format('delta').table(users_to_reprocess_table)
    users_to_reprocess_df.createOrReplaceTempView('past_users_to_reprocess')
    while current_iteration < max_iterations:
        print(f'Loading past data for {timestamp_partition_start} and {timestamp_partition_end}')
        new_time_candidates = spark_session.sql(f'''
            SELECT * FROM (
                SELECT
                    v.*,
                    SUM(
                    CASE WHEN unix_timestamp(LEAD(event_time, 1, utp.min_event_time) OVER (PARTITION BY v.user_id ORDER BY event_time  )) - unix_timestamp(event_time) >= 600
                         THEN 1
                         ELSE 0
                    END
                ) OVER (PARTITION BY v.user_id ORDER BY event_time DESC) + 1 AS session_id
                FROM {VISITS_TABLE} v
                JOIN past_users_to_reprocess utp ON utp.user_id = v.user_id
                WHERE v.event_time >= "{timestamp_partition_end.strftime(date_format)}" AND event_time < "{timestamp_partition_start.strftime(date_format)}"
            ) x WHERE session_id = 1
        ''').drop('session_id')
        new_time_candidates.cache()
        if new_time_candidates.isEmpty():
            break

        new_time_candidates.write.format('delta').mode('append').saveAsTable(output_table_name)

        new_time_candidates.groupBy('user_id').agg(
            F.min('event_time').alias('min_event_time')
        ).createOrReplaceTempView('past_users_to_reprocess')

        new_time_candidates.unpersist()
        timestamp_partition_start = timestamp_partition_end
        timestamp_partition_end = timestamp_partition_end - datetime.timedelta(hours=1)
        current_iteration += 1


def get_data_to_reprocess_from_future_partitions(spark_session: SparkSession, users_to_reprocess_table: str,
                                               current_partition: str, output_table_name: str,
                                               max_iterations: int = 100):
    date_format = '%Y-%m-%dT%H:%M:%S.%f'
    timestamp_partition_start = datetime.datetime.strptime(current_partition, date_format)
    timestamp_partition_end = datetime.datetime.strptime(current_partition, date_format) + datetime.timedelta(hours=1)
    current_iteration = 1
    users_to_reprocess_df = spark_session.read.format('delta').table(users_to_reprocess_table)
    users_to_reprocess_df.createOrReplaceTempView('past_users_to_reprocess')
    while current_iteration < max_iterations:
        print(f'Loading future data for {timestamp_partition_start} and {timestamp_partition_end}')
        new_time_candidates = spark_session.sql(f'''
            SELECT * FROM (
                SELECT
                    v.*,
                    SUM(
                        CASE WHEN unix_timestamp(event_time) - unix_timestamp(LAG(event_time, 1, utp.max_event_time) OVER (PARTITION BY v.user_id ORDER BY event_time ASC)) > 600
                         THEN 1
                         ELSE 0
                    END
                ) OVER (PARTITION BY v.user_id ORDER BY event_time ASC) + 1 AS session_id
                FROM {VISITS_TABLE} v
                JOIN past_users_to_reprocess utp ON utp.user_id = v.user_id
                WHERE v.event_time >= "{timestamp_partition_start.strftime(date_format)}" AND event_time < "{timestamp_partition_end.strftime(date_format)}"
            ) x WHERE session_id = 1
            ''').drop('session_id')

        new_time_candidates.cache()
        if new_time_candidates.isEmpty():
            break

        new_time_candidates.write.format('delta').mode('append').saveAsTable(output_table_name)

        new_time_candidates.groupBy('user_id').agg(
            F.max('event_time').alias('max_event_time')
        ).createOrReplaceTempView('past_users_to_reprocess')

        new_time_candidates.unpersist()
        timestamp_partition_start = timestamp_partition_end
        timestamp_partition_end = timestamp_partition_end + datetime.timedelta(hours=1)
        current_iteration += 1