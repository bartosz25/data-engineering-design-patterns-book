from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType

from scylla_db_writer import ScyllaDbWriter
from visits_mapper import map_visit_to_session

if __name__ == "__main__":
    base_dir = '/tmp/dedp/ch04/database/keyed-kafka-spark'

    spark_session = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .config('spark.sql.shuffle.partitions', 2).getOrCreate()

    input_data = (spark_session.readStream.format('kafka')
                  .option('kafka.bootstrap.servers', 'localhost:9094')
                  .option('subscribe', 'visits')
                  .option('startingOffsets', 'EARLIEST')
                  .load())

    grouped_visits = (input_data.selectExpr('CAST(value AS STRING)', 'timestamp')
                      .select(F.from_json(F.col('value'), 'user_id LONG, page STRING, event_time TIMESTAMP')
                               .alias('visit'), F.col('timestamp'))
                      .selectExpr('visit.*', 'UNIX_TIMESTAMP(timestamp) AS append_time')
                      .withWatermark('event_time', '10 seconds')
                      .groupBy(F.col('user_id')))

    letters_with_ids = grouped_visits.applyInPandasWithState(
        func=map_visit_to_session,
        outputStructType=StructType([
            StructField('session_id', LongType()),
            StructField('user_id', LongType()),
            StructField('pages', ArrayType(StringType(), False))
        ]),
        stateStructType=StructType([
            StructField('min_append_time', LongType()),
            StructField('pages', ArrayType(StringType(), False))
        ]),
        outputMode='update',
        timeoutConf='EventTimeTimeout'
    )

    # This is a workaround for the following exception:
    # Caused by: java.lang.ClassCastException: org.apache.spark.sql.vectorized.ColumnarBatchRow cannot be cast to org.apache.spark.sql.catalyst.expressions.UnsafeRow
    def write_sessions_to_scylla_db(sessions_dataframe: DataFrame, micro_batch_version: int):
        def write_to_scylla_db(sessions_to_write):
            scylladb_writer = ScyllaDbWriter()
            for user_session in sessions_to_write:
                scylladb_writer.process(user_session)
            scylladb_writer.close()

        sessions_dataframe.foreachPartition(write_to_scylla_db)

    write_query = (letters_with_ids.writeStream.outputMode('update')
                   .option('checkpointLocation', f'{base_dir}/checkpoint')
                   .foreachBatch(write_sessions_to_scylla_db)
                   .start())

    write_query.awaitTermination()
