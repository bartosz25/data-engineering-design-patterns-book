import os
import sys
from pathlib import Path

import pyspark.sql.protobuf.functions as F
from pyspark.sql import SparkSession

from dataframe_printer import printer_sink_v2

sys.path.append(f'{os.getcwd()}/protobuf_output/python')

if __name__ == '__main__':
    def read_binary_descriptor_from_path(path: Path):
        with path.open("rb") as f:
            binary_descriptor_data = f.read()
        return binary_descriptor_data


    protobuf_descriptor_path = Path(f'{os.getcwd()}/protobuf_output/visit.bin')

    spark = (SparkSession.builder.master('local[2]')
             .config('spark.jars.packages',
                     'org.apache.spark:spark-protobuf_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
             .getOrCreate())

    input_data_stream = (spark.readStream
                         .option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('subscribe', 'visits')
                         .option('startingOffsets', 'EARLIEST')
                         .format('kafka').load())

    visits_to_observe = (input_data_stream.select(
        F.from_protobuf(data='value', messageName='VisitV2',
                        binaryDescriptorSet=read_binary_descriptor_from_path(protobuf_descriptor_path))
        .alias('visit')
    ))

    write_query = (visits_to_observe.writeStream
                   .option('checkpointLocation',
                           '/tmp/dedp/ch09/02-schema-consistency/02-schema-migrator-protobuf-apache-spark/checkpoint')
                   .foreachBatch(printer_sink_v2))

    write_query.start().awaitTermination()
