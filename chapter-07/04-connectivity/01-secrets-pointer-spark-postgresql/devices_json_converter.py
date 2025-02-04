import localstack_client.session as boto3
from pyspark.sql import SparkSession

if __name__ == '__main__':
    output_dir = '/tmp/dedp/ch07/04-connectivity/01-secrets-pointer-spark-postgresql/devices-json'

    secretsmanager_client = boto3.client('secretsmanager')
    db_user = secretsmanager_client.get_secret_value(SecretId='psql_user')['SecretString']
    db_password = secretsmanager_client.get_secret_value(SecretId='psql_password')['SecretString']

    spark = (SparkSession.builder.master('local[*]')
             .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.10').getOrCreate())

    input_data = spark.read.option('driver', 'org.postgresql.Driver').jdbc(
        url='jdbc:postgresql:dedp', table='dedp.devices',
        properties={'user': db_user, 'password': db_password}
    )

    input_data.write.mode('overwrite').json(output_dir)

    # read the written dataset; omitted schema for the sake of simplicity but you should
    # always define it for semi-structured file formats to avoid potentially costly schema inference step
    spark.read.json(output_dir).show()
