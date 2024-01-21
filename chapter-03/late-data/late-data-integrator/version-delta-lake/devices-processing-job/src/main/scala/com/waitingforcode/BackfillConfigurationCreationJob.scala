package com.waitingforcode

import org.rogach.scallop.ScallopConf
import org.rogach.scallop._
object BackfillConfigurationCreationJob {

  private class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val outputLocation = opt[String](required = true)
    val tableFullPath = opt[String](required = true)
    val outputBackfillingFileName = opt[String](required = true)
    val partitionsToExclude = opt[List[String]](required = true)

    verify()
  }
  def main(args: Array[String]): Unit = {
    val jobArguments = new Args(args)

    val outputLocation = jobArguments.outputLocation.toOption.get
    val lastProcessedVersion = LastProcessedVersion.load(outputLocation)

    val sparkSessionFactory = SparkSessionFactoryInitializer.init(outputLocation)
    val sparkSession = sparkSessionFactory.sparkSession

    val partitionsHandler = new PartitionsHandler(sparkSession,
      jobArguments.tableFullPath.toOption.get, lastProcessedVersion + 1)
    val unprocessedPartitions = partitionsHandler.listUnprocessedPartitions()

    val excludedPartitionsSet = jobArguments.partitionsToExclude.toOption.get.toSet
    val partitionsToBackfill: Seq[Map[String, String]] = unprocessedPartitions.filter(partitions => {
      val partition = partitions.map(pair => s"${pair._1}=${pair._2}").mkString("/")
      !excludedPartitionsSet.contains(partition)
    }).toSeq

    val configFullPath = s"${outputLocation}/${jobArguments.outputBackfillingFileName.toOption.get}"
    PartitionsToBackfill.save(configFullPath,
      PartitionsToBackfill.BackfillConfiguration(
        partitions = partitionsToBackfill,
        lastProcessedVersion = partitionsHandler.lastTableVersion)
    )
  }

}

