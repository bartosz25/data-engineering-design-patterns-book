package com.waitingforcode.jobs

import com.waitingforcode.{PartitionsStateTable, SparkSessionFactoryInitializer}
import org.apache.spark.sql.delta.DeltaLog
import scopt.OParser

/**
 * The job simulates data processing and updates the Delta Lake last processed version
 * associated to the partition.
 */
object DataProcessorAndPartitionMarkerAsProcessed {

  case class Args(outputLocationBaseDir: String, tableFullPath: String, partition: String) {}

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Args]
    val parser1 = {
      import builder._
      OParser.sequence(
        opt[String]("outputLocationBaseDir")
          .action((outputLocation, c) => c.copy(outputLocationBaseDir = outputLocation))
          .text("Output location directory"),
        opt[String]("partition")
          .action((partition, c) => c.copy(partition = partition))
          .text("Current partition. Expected format: partition_field=1"),
        opt[String]("tableFullPath")
          .action((tableFullPath, c) => c.copy(tableFullPath = tableFullPath))
          .text("Devices table full path"),
      )
    }
    OParser.parse(parser1, args, Args(null, null, null)) match {
      case Some(config) =>
        val outputLocation = config.outputLocationBaseDir
        val sparkSessionFactory = SparkSessionFactoryInitializer.init(outputLocation)

        val sparkSession = sparkSessionFactory.sparkSession
        val dataProcessingVersion = DeltaLog.forTable(sparkSession, config.tableFullPath)
          .unsafeVolatileSnapshot.version
        println(s"Processing current partition: ${config.partition} for version ${dataProcessingVersion}")
        println(s"Marking partition as completed ${config.partition}")

        val partitionsStateTable = new PartitionsStateTable(sparkSession, outputLocation)

        partitionsStateTable.markPartitionAsAlreadyProcessed(config.partition, dataProcessingVersion)
    }
  }
}
