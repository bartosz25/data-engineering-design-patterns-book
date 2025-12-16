from dataclasses import dataclass

from pyspark.sql import SparkSession

from reprocessing_data_fetchers import get_data_to_reprocess_from_future_partitions, \
    get_data_to_reprocess_from_past_partitions, get_users_to_reprocess


@dataclass
class ReprocessingDataPreparator:
    start_time: str
    end_time: str
    reprocessing_id: str
    users_to_reprocess_table: str | None = None
    visits_to_reprocess_table: str | None = None

    def prepare_datasets_for_reprocessing(self, spark_session: SparkSession):
        self._prepare_users_table(spark_session)
        self._prepare_visits_to_reprocess_table(spark_session)

    def _prepare_users_table(self, spark_session: SparkSession):
        self.users_to_reprocess_table = f'users_to_reprocess_{self.reprocessing_id}'
        spark_session.sql(f'DROP TABLE IF EXISTS {self.users_to_reprocess_table}')
        users_to_reprocess = get_users_to_reprocess(spark_session, self.start_time, self.end_time)
        users_to_reprocess.write.format('delta').mode('overwrite').saveAsTable(self.users_to_reprocess_table)


    def _prepare_visits_to_reprocess_table(self, spark_session: SparkSession):
        self.visits_to_reprocess_table = f'visits_reprocessing_{self.reprocessing_id}'
        spark_session.sql(f'DROP TABLE IF EXISTS {self.visits_to_reprocess_table}')
        get_data_to_reprocess_from_future_partitions(
            spark_session=spark_session, output_table_name=self.visits_to_reprocess_table,
            users_to_reprocess_table=self.users_to_reprocess_table,
            current_partition=self.end_time)

        get_data_to_reprocess_from_past_partitions(
            spark_session=spark_session, output_table_name=self.visits_to_reprocess_table,
            users_to_reprocess_table=self.users_to_reprocess_table,
            current_partition=self.start_time
        )