package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

class PartitionsStateTable(sparkSession: SparkSession, baseDir: String) {

  import sparkSession.implicits._
  private val TablePath = s"${baseDir}/partitions-state"
  sparkSession.sql(
    s"""
      |CREATE TABLE IF NOT EXISTS delta.`${TablePath}` (
      |  partition STRING,
      |  isProcessed BOOLEAN,
      |  lastProcessedVersion LONG
      |) USING DELTA
      |""".stripMargin)

  def getPartitionsToBackfill(lastVersionPerPartition: Map[String, Long],
                              currentPartition: String): Seq[String] = {
    import sparkSession.implicits._
    val partitionsToBackfill = sparkSession.read.format("delta").load(TablePath)
      .select("partition", "isProcessed", "lastProcessedVersion")
      .as[PartitionState]
      .filter(state => state.lastProcessedVersion.isDefined)
      .filter(state => {
        (!lastVersionPerPartition.contains(state.partition) ||
        lastVersionPerPartition(state.partition) > state.lastProcessedVersion.get) &&
          !state.isProcessed && state.partition < currentPartition
      })
    partitionsToBackfill.map(_.partition).collect()
  }

  def markPartitionAsCurrentlyProcessing(partition: String): Unit = {
    markPartitionsAsCurrentlyProcessing(Seq(partition))
  }

  def markPartitionsAsCurrentlyProcessing(partitions: Seq[String]): Unit = {
    val partitionsToUpdate = partitions.map(partition => (partition, true)).toDF("n_partition", "state")
    DeltaTable.forPath(TablePath)
      .merge(partitionsToUpdate.as("new"), "partition = n_partition")
      .whenMatched().updateExpr(Map("isProcessed" -> "true"))
      .whenNotMatched().insert(Map("partition" -> $"n_partition", "isProcessed" -> $"state"))
      .execute()
  }

  def markPartitionsAsAlreadyProcessed(partition: String, version: Long): Unit = {
    DeltaTable.forPath(TablePath).updateExpr(
      condition = s"partition = '${partition}'",
      set = Map("isProcessed" -> "false", "lastProcessedVersion" -> s"${version}")
    )
  }

}

case class PartitionState(partition: String, isProcessed: Boolean, lastProcessedVersion: Option[Long])