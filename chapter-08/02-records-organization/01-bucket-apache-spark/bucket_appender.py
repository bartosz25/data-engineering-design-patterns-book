from pyspark.sql import SparkSession

from config import get_base_dir

if __name__ == "__main__":
    base_dir = get_base_dir()

    spark_session = (SparkSession.builder.master("local[*]").enableHiveSupport()
        .config("spark.sql.warehouse.dir", f'{base_dir}/warehouse').getOrCreate())

    buckets_number = 5
    datasets_with_bucket_keys = [
        (f'{base_dir}/input-users', 'users', 'id'),
        (f'{base_dir}/input-visits', 'visits', 'visit_id'),
    ]
    for dataset, table_name, bucket_key in datasets_with_bucket_keys:
        input_dataset = spark_session.read.json(dataset)
        input_dataset.write.bucketBy(buckets_number, bucket_key).mode('append').saveAsTable(table_name)

    # the devices table is not bucketed (clustered);
    #spark_session.read.json(f'{base_dir}/input-devices').write.mode('overwrite').saveAsTable('devices')
