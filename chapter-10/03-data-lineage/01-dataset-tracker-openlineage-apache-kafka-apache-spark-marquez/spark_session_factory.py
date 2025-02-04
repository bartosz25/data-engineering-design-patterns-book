from pyspark.sql import SparkSession


def create_spark_session_with_open_lineage(app_name: str) -> SparkSession:
    return (SparkSession.builder.master('local[*]')
            .appName(app_name)
            .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
            .config('spark.openlineage.transport.type', 'http')
            .config('spark.openlineage.transport.url', 'http://localhost:5000')
            .config('spark.openlineage.namespace', 'visits')
            .config('spark.jars.packages', 'io.openlineage:openlineage-spark_2.12:1.21.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
            .getOrCreate())
