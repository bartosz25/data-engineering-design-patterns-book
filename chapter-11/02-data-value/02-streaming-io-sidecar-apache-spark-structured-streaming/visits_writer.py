from delta import DeltaTable
from pyspark.sql import DataFrame, functions as F

from config import get_base_dir, get_devices_stats_table_dir, get_visits_enriched_raw_table_dir


def write_enriched_visits(visits_enriched: DataFrame, batch_number: int):
    visits_enriched.persist()

    write_options = {'txnVersion': str(batch_number), 'txnAppId': 'app-1'}
    (visits_enriched.write.format('delta').mode('append').options(**write_options)
     .save(get_visits_enriched_raw_table_dir()))

    devices_stats = (visits_enriched.groupBy('type', 'version').count()
                     .withColumn('batch_id', F.lit(batch_number)))

    # as we use the idempotent writer, we don't need to MERGE here; in case of restart, the job will retry
    # only the failed writers, so there won't be duplicates
    (devices_stats.write.format('delta').options(**write_options).mode('append')
     .save(get_devices_stats_table_dir()))

    visits_enriched.unpersist()