package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object FilterWithStatsInterceptor {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val schema = "type STRING, full_name STRING, version STRING"

    val inputDataset = sparkSession.read.schema(schema).json("/tmp/dedp/ch03/filter-interceptor-scala/input/dataset.json")
    val typeIsNullAccumulator = sparkSession.sparkContext.longAccumulator("typeIsNull")
    val typeIsTooShortAccumulator = sparkSession.sparkContext.longAccumulator("typeIsTooShort")
    val fullNameIsNullAccumulator = sparkSession.sparkContext.longAccumulator("fullNameIsNull")
    val versionIsNullAccumulator = sparkSession.sparkContext.longAccumulator("versionIsNull")
    def wrappedFilterWithAccumulator(filter: (Device) => Boolean,
                                     device: Device, accumulator: LongAccumulator): Boolean = {
      val result = filter(device)
      if (!result) {
        accumulator.add(1)
      }
      result
    }

    val filteredDevices = inputDataset.as[Device]
      .filter(device => wrappedFilterWithAccumulator(((d) => d.`type` != null), device, typeIsNullAccumulator))
      .filter(device => wrappedFilterWithAccumulator(((d) => d.`type`.length > 1), device, typeIsTooShortAccumulator))
      .filter(device => wrappedFilterWithAccumulator(((d) => d.full_name != null), device, fullNameIsNullAccumulator))
      .filter(device => wrappedFilterWithAccumulator(((d) => d.version != null), device, versionIsNullAccumulator))

    filteredDevices.count()

    println(s"Devices without type: ${typeIsNullAccumulator.value}")
    println(s"Devices with too short name: ${typeIsTooShortAccumulator.value}")
    println(s"Devices without full name: ${fullNameIsNullAccumulator.value}")
    println(s"Devices without version: ${versionIsNullAccumulator.value}")
  }
}

case class Device(`type`: String, full_name: String, version: String)