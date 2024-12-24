package com.waitingforcode.jobs

import com.waitingforcode.{DataProcessingJobExecutionConfiguration, PartitionsHandler, PartitionsStateTable, SparkSessionFactoryInitializer}
import scopt.OParser

object PartitionStateInProgressMarkerJob {

  case class Args(outputLocationBaseDir: String, partitionToUpdate: String) {

  }

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Args]
    val parser1 = {
      import builder._
      OParser.sequence(
        opt[String]("outputLocationBaseDir")
          .action((outputLocation, c) => c.copy(outputLocationBaseDir = outputLocation))
          .text("Output location directory"),
        opt[String]("partition")
          .action((partition, c) => c.copy(partitionToUpdate = partition))
          .text("Partition to update. Expected format: partition_field=1")
      )
    }
    OParser.parse(parser1, args, Args(null, null)) match {
      case Some(config) =>
        val outputLocation = config.outputLocationBaseDir

        val sparkSessionFactory = SparkSessionFactoryInitializer.init(outputLocation)

        val sparkSession = sparkSessionFactory.sparkSession
        val partitionsStateTable = new PartitionsStateTable(sparkSession, outputLocation)
        partitionsStateTable.markPartitionAsBeingProcessed(config.partitionToUpdate)
    }
  }

}

