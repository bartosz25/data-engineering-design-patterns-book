package com.waitingforcode.jobs

import com.waitingforcode.{PartitionsHandler, PartitionsStateTable, PartitionsToBackfill, SparkSessionFactoryInitializer}
import scopt.OParser

/**
 * The first job of the workflow. It detects all partitions with late data and writes a backfilling
 * file for Apache Airflow with the following content:
 *  ```
 *  {"partitions": [
 *    "2024-10-03", "2024-10-04"
 *  ]}
 *  ```
 */
object LateDataDetectionJob {

  case class Args(outputLocationBaseDir: String, tableFullPath: String,
                  outputBackfillingFileName: String, currentPartition: String) {
    val backfillConfigFullPath = s"${outputLocationBaseDir}/${outputBackfillingFileName}"
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
        val outputLocation = config.outputLocationBaseDir
        val sparkSessionFactory = SparkSessionFactoryInitializer.init(outputLocation)
        val sparkSession = sparkSessionFactory.sparkSession

        val partitionsHandler = new PartitionsHandler(sparkSession, config.tableFullPath)

        val lastVersionPerPartition: Map[String, Long] = partitionsHandler.getLastVersionForEachPartition

        val partitionsStateTable = new PartitionsStateTable(sparkSession, outputLocation)
        val partitionsToBackfill = partitionsStateTable.getPartitionsToBackfill(
          lastVersionPerPartition, config.currentPartition
        )

        // Create the file first; if the write fails, we won't update the partitions
        PartitionsToBackfill.save(config.backfillConfigFullPath,
          PartitionsToBackfill.BackfillConfiguration(partitions = partitionsToBackfill)
        )

        partitionsStateTable.markPartitionsAsBeingProcessed(partitionsToBackfill)
    }
  }

}

