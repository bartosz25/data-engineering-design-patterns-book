from pyspark.sql import SparkSession

if __name__ == '__main__':
    output_dir = '/tmp/dedp/ch07/04-connectivity/02-secretless-connector-apache-spark-postgresql/devices-json'

    spark = (SparkSession.builder.master('local[*]')
             .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.10').getOrCreate())

    input_data = spark.read.option('driver', 'org.postgresql.Driver').jdbc(
        url='jdbc:postgresql:dedp', table='dedp.devices',
        properties={
            'ssl': 'true', 'sslmode': 'verify-full', 'user': 'dedp_test',
            'sslrootcert': 'dataset/certs/ssl-cert-snakeoil.pem',
        }
    )

    input_data.write.mode('overwrite').json(output_dir)

    # read the written dataset; omitted schema for the sake of simplicity but you should
    # always define it for semi-structured file formats to avoid potentially costly schema inference step
    spark.read.json(output_dir).show()
