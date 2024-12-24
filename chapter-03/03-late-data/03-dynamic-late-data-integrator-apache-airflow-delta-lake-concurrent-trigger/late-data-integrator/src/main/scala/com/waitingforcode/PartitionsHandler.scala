package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}

class PartitionsHandler(sparkSession: SparkSession, tablePath: String) {

  lazy  val deltaLog = DeltaLog.forTable(sparkSession, tablePath)
  lazy val lastVersionOfTheTable: Long = {
    deltaLog.unsafeVolatileSnapshot.version
  }

  // We could generate these values incrementally in a different job; for the sake of simplicity,
  // to stay focused on the late data integration, we're going to issue the partitions mapping query
  // each time
  def getLastVersionForEachPartition: Map[String, Long] = {
    val partitionsWithChangeVersions: Iterator[(String, Long)] = deltaLog.getChanges(0).flatMap {
      case (version, actions) => {
        val changedPartitionsInVersion: Set[String] = actions.map {
          case addFile: AddFile if addFile.dataChange => Some(addFile.partitionValues.map(e => s"${e._1}=${e._2}").mkString("/")) // TODO: change
          case removeFile: RemoveFile if removeFile.dataChange => Some(removeFile.partitionValues.map(e => s"${e._1}=${e._2}").mkString("/")) // TODO: change
          case _ => None
        }.filter(_.isDefined).map(_.get).toSet

        changedPartitionsInVersion.map(partition => (partition, version))
      }
    }
    val lastVersionForEachPartition: Map[String, Long] = partitionsWithChangeVersions.toSeq.groupBy(pair => pair._1)
      .mapValues(pairs => pairs.map(_._2).max)

    // https://stackoverflow.com/questions/77846427/task-not-serializable-immutable-maplikeanon2-scala
    // https://github.com/apache/spark/commit/1ab0f02b3e755c6ec057d6561ffb6ddd7e53c7cd
    // Using map(identity).toMap to avoid serialization issues
    lastVersionForEachPartition.map(identity).toMap
  }

}
