package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class PartitionsStateTableWritingTest extends AnyFlatSpec with Matchers with SparkSessionSpec {

  import sparkSession.implicits._

  behavior of "partitions to backfill"

  it should "add and later update the existing partition state" in {
    val outputDir = "/tmp/test_partitions_handler_4"
    FileUtils.deleteDirectory(new File(outputDir))
    val stateTable = new PartitionsStateTable(sparkSession = sparkSession, baseDir = outputDir)

    stateTable.markPartitionAsBeingProcessed("date=2024-06-06")

    val partitions = sparkSession.read.format("delta").load(s"${outputDir}/partitions-state").as[PartitionState].collect()

    partitions should have size 1
    partitions(0) shouldEqual PartitionState("date=2024-06-06", true, None)

    stateTable.markPartitionsAsProcessed("date=2024-06-06", 1)
    val partitionsAfterUpdate = sparkSession.read.format("delta").load(s"${outputDir}/partitions-state").as[PartitionState].collect()

    partitionsAfterUpdate should have size 1
    partitionsAfterUpdate(0) shouldEqual PartitionState("date=2024-06-06", false, Some(1L))

    stateTable.markPartitionAsBeingProcessed("date=2024-06-06")

    val partitionsAfterProcessingUpdate = sparkSession.read.format("delta").load(s"${outputDir}/partitions-state").as[PartitionState].collect()

    partitionsAfterProcessingUpdate should have size 1
    partitionsAfterProcessingUpdate(0) shouldEqual PartitionState("date=2024-06-06", true, Some(1L))
  }

}
