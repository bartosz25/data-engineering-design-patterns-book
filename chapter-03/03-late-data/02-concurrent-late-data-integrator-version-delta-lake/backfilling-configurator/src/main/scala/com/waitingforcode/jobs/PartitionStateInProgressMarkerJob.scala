package com.waitingforcode.jobs

import com.waitingforcode.{DataProcessingJobExecutionConfiguration, PartitionsHandler, PartitionsStateTable, SparkSessionFactoryInitializer}
import scopt.OParser

object PartitionStateInProgressMarkerJob {

  case class Args(outputLocationBaseDir: String, tableFullPath: String, outputConfigurationFileName: String,
                  partitionToUpdate: String) {

    lazy val jobConfigurationFileFullPath = s"${outputLocationBaseDir}/${outputConfigurationFileName}"

  }

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
          .text("Devices table full path"),
        opt[String]("partition")
          .action((partition, c) => c.copy(partitionToUpdate = partition))
          .text("Partition to update. Expected format: partition_field=1"),
        opt[String]("outputFileName")
          .action((fileName, c) => c.copy(outputConfigurationFileName = fileName))
          .text("Name of the execution file"),
      )
    }
    OParser.parse(parser1, args, Args(null, null, null, null)) match {
      case Some(config) =>
        val outputLocation = config.outputLocationBaseDir

        val sparkSessionFactory = SparkSessionFactoryInitializer.init(outputLocation)

        val sparkSession = sparkSessionFactory.sparkSession
        val partitionsStateTable = new PartitionsStateTable(sparkSession, outputLocation)
        val partitionsHandler = new PartitionsHandler(sparkSession, config.tableFullPath)
        val lastVersionOfTheTable = partitionsHandler.lastVersionOfTheTable

        // Write the file first; if the table update fails, the file will be useless as the
        // pipeline won't move on
        DataProcessingJobExecutionConfiguration(config.partitionToUpdate, lastVersionOfTheTable)
          .writeAsFile(config.jobConfigurationFileFullPath)

        partitionsStateTable.markPartitionAsCurrentlyProcessing(config.partitionToUpdate)

      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }

}

