from pyspark.sql import SparkSession, DataFrame

if __name__ == "__main__":
    base_dir = '/tmp/dedp/ch05/03-data-combination/01-distributed-json-postgresql/visits'

    spark_session = (SparkSession.builder.master('local[*]')
                     .config("spark.sql.session.timeZone", "UTC")
                     .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.10')
                     .config('spark.sql.autoBroadcastJoinThreshold', -1)  # disable broadcast join for the demo
                     .getOrCreate())

    visits: DataFrame = spark_session.read.json(f'{base_dir}/input-visits')

    devices: DataFrame = spark_session.read.jdbc(url='jdbc:postgresql:dedp', table='dedp.devices',
                                                 properties={'user': 'dedp_test', 'password': 'dedp_test',
                                                             'driver': 'org.postgresql.Driver'})

    visits_with_devices = visits.join(devices,
                                      [devices.type == visits.context.technical.device_type,
                                       devices.version == visits.context.technical.device_version],
                                      'inner')

    visits_with_devices.explain(extended=True)

    visits_with_devices.show(truncate=False)
