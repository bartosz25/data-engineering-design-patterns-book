package com.waitingforcode

import scopt.OParser

object LateDataPartitionsCreationJob {

  case class Args(outputLocation: String, tableFullPath: String,
                  lateDataConfigurationFileName: String, currentPartition: String)

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Args]
    val parser1 = {
      import builder._
      OParser.sequence(
        opt[String]("outputLocation")
          .action((outputLocation, c) => c.copy(outputLocation = outputLocation))
          .text("Output location directory"),
        opt[String]("tableFullPath")
          .action((tableFullPath, c) => c.copy(tableFullPath = tableFullPath))
          .text("Devices table full path"),
        opt[String]("lateDataConfigurationFileName")
          .action((lateDataConfigurationFileName, c) => c.copy(lateDataConfigurationFileName = lateDataConfigurationFileName))
          .text("Name of the late data configuration file"),
        opt[String]("currentPartition")
          .action((currentPartition, c) => c.copy(currentPartition = currentPartition))
          .text("Current partition. Expected format: partition_field=1"),
      )
    }
    OParser.parse(parser1, args, Args(null, null, null, null)) match {
      case Some(config) =>

        val outputLocation = config.outputLocation
        val lastProcessedVersion = LastProcessedVersion.load(outputLocation)

        val sparkSessionFactory = SparkSessionFactoryInitializer.init(outputLocation)
        val sparkSession = sparkSessionFactory.sparkSession

        val partitionsHandler = new PartitionsHandler(sparkSession,
          config.tableFullPath, lastProcessedVersion + 1)
        val unprocessedPartitions = partitionsHandler.listUnprocessedPartitions()

        val partitionsToBackfill: Seq[Map[String, String]] = unprocessedPartitions.filter(partitions => {
          val partition = partitions.map(pair => s"${pair._1}=${pair._2}").mkString("/")
          partition < config.currentPartition
        })

        val configFullPath = s"${outputLocation}/${config.lateDataConfigurationFileName}"
        PartitionsToBackfill.save(configFullPath,
          PartitionsToBackfill.BackfillConfiguration(
            partitions = partitionsToBackfill,
            lastProcessedVersion = partitionsHandler.lastTableVersion)
        )
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }

}

