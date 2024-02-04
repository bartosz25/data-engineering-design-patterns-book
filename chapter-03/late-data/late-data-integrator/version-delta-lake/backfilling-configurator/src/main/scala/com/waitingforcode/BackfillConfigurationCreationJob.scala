package com.waitingforcode

import scopt.OParser

object BackfillConfigurationCreationJob {

  case class Args(outputLocation: String, tableFullPath: String,
                  outputBackfillingFileName: String, currentPartition: String)

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
        opt[String]("outputBackfillingFileName")
          .action((outputBackfillingFileName, c) => c.copy(outputBackfillingFileName = outputBackfillingFileName))
          .text("Name of the backfilling file"),
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

        val configFullPath = s"${outputLocation}/${config.outputBackfillingFileName}"
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

