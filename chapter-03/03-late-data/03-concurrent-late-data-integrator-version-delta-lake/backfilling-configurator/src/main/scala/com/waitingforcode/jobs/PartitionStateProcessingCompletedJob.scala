package com.waitingforcode.jobs

import com.waitingforcode.{DataProcessingJobExecutionConfiguration, PartitionsStateTable, SparkSessionFactoryInitializer}
import scopt.OParser

object PartitionStateProcessingCompletedJob {

  case class Args(outputLocationBaseDir: String, jobExecutionFileName: String) {

    lazy val jobConfigurationFileFullPath = s"${outputLocationBaseDir}/${jobExecutionFileName}"

  }

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Args]
    val parser1 = {
      import builder._
      OParser.sequence(
        opt[String]("outputLocationBaseDir")
          .action((outputLocation, c) => c.copy(outputLocationBaseDir = outputLocation))
          .text("Output location directory"),
        opt[String]("jobExecutionFileName")
          .action((fileName, c) => c.copy(jobExecutionFileName = fileName))
          .text("Name of the execution file"),
      )
    }
    OParser.parse(parser1, args, Args(null, null)) match {
      case Some(config) =>
        val outputLocation = config.outputLocationBaseDir

        val sparkSessionFactory = SparkSessionFactoryInitializer.init(outputLocation)

        val jobExecutionConfiguration = DataProcessingJobExecutionConfiguration.readFromFile(
          config.jobConfigurationFileFullPath
        )

        val sparkSession = sparkSessionFactory.sparkSession
        val partitionsStateTable = new PartitionsStateTable(sparkSession, outputLocation)

        partitionsStateTable.markPartitionsAsAlreadyProcessed(jobExecutionConfiguration.partition,
          jobExecutionConfiguration.version)

        DataProcessingJobExecutionConfiguration.removeFile(config.jobConfigurationFileFullPath)
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }

}

