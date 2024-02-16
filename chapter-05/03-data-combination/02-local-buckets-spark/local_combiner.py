from pyspark.sql import SparkSession

from config import get_base_dir

if __name__ == "__main__":
    base_dir = get_base_dir()

    spark_session = (SparkSession.builder.master('local[*]').enableHiveSupport()
                     .config('spark.sql.autoBroadcastJoinThreshold', -1) # disable broadcast join for the demo
                     .config('spark.sql.warehouse.dir', f'{base_dir}/warehouse').getOrCreate())

    spark_session.sql('''
        SELECT v.visit_id, v.user_id, u.login FROM visits v 
        JOIN users u ON u.id = v.user_id
    ''').explain(extended=True)

    spark_session.sql('''
        SELECT v.visit_id, v.user_id, d.full_name FROM visits v 
        JOIN devices d ON d.type = v.context.technical.device_type 
            AND v.context.technical.device_version = d.version
    ''').explain(extended=True)
