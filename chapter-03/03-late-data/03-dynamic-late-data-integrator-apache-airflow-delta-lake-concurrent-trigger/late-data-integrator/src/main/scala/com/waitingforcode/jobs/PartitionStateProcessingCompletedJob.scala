package com.waitingforcode.jobs

import com.waitingforcode.{DataProcessingJobExecutionConfiguration, PartitionsStateTable, SparkSessionFactoryInitializer}
import org.apache.spark.sql.delta.DeltaLog
import scopt.OParser

object PartitionStateProcessingCompletedJob {

  case class Args(outputLocationBaseDir: String, tableFullPath: String, partition: String) {}

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Args]
    val parser1 = {
      import builder._
      OParser.sequence(
        opt[String]("outputLocationBaseDir")
          .action((outputLocation, c) => c.copy(outputLocationBaseDir = outputLocation))
          .text("Output location directory"),
        opt[String]("tableFullPath")
          .action((tableFullPath, c) => c.copy(tableFullPath = tableFullPath))
          .text("Data table path"),
        opt[String]("partition")
          .action((partition, c) => c.copy(partition = partition))
          .text("Current partition")
      )
    }
    OParser.parse(parser1, args, Args(null, null, null)) match {
      case Some(config) =>
        val outputLocation = config.outputLocationBaseDir

        val sparkSessionFactory = SparkSessionFactoryInitializer.init(outputLocation)

        println("Processing the data...")

        val sparkSession = sparkSessionFactory.sparkSession
        val partitionsStateTable = new PartitionsStateTable(sparkSession, outputLocation)
        val dataProcessingVersion = DeltaLog.forTable(sparkSession, config.tableFullPath)
          .unsafeVolatileSnapshot.version
        partitionsStateTable.markPartitionsAsProcessed(config.partition,
          dataProcessingVersion)
    }
  }

}

