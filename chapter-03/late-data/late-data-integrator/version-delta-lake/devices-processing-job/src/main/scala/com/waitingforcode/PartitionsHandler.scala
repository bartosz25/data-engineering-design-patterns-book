package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}

class PartitionsHandler(sparkSession: SparkSession, tablePath: String,
  firstUnprocessedVersion: Long) {

  lazy  val deltaLog = DeltaLog.forTable(sparkSession, tablePath)
  lazy val lastTableVersion = deltaLog.unsafeVolatileSnapshot.version

  def listUnprocessedPartitions(): Iterator[Map[String, String]] = {

    val changedPartitionsFromLastVersion: Iterator[Map[String,String]] = deltaLog
      .getChanges(firstUnprocessedVersion).map(files => {
      files._2
    }).flatMap(actions => {
      actions.map(action => {
        val impactedPartitions: Map[String, String] = action match {
          case addFile: AddFile =>  addFile.partitionValues
          case removeFile: RemoveFile => removeFile.partitionValues
          case _ => Map.empty[String, String]
        }
        impactedPartitions
      })
    }).filter(_.nonEmpty).distinct
    changedPartitionsFromLastVersion
  }

}
