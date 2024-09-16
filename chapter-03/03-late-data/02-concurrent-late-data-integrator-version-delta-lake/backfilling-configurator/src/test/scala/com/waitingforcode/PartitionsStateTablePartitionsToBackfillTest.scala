package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class PartitionsStateTablePartitionsToBackfillTest extends AnyFlatSpec with Matchers with SparkSessionSpec {

  import sparkSession.implicits._

  behavior of "partitions to backfill"

  it should "return empty list if there is no partitions processed previously" in {
    val outputDir = "/tmp/test_partitions_handler_5"
    FileUtils.deleteDirectory(new File(outputDir))
    val stateTable = new PartitionsStateTable(sparkSession = sparkSession, baseDir = outputDir)

    stateTable.getPartitionsToBackfill(Map.empty, "date=2024-05-06") shouldBe empty
  }

  it should "return partitions to backfill that are older than the versions, not currently processed," +
    " and different than the current partition" in {
    val outputDir = "/tmp/test_partitions_handler_6"
    FileUtils.deleteDirectory(new File(outputDir))
    Seq(
      PartitionState("date=2024-05-04", false, None), // Wasn't processed yet
      PartitionState("date=2024-05-05", false, Some(5L)), // Version same as recently processed
      PartitionState("date=2024-05-06", false, Some(5L)), //Good to backfill
      PartitionState("date=2024-05-07", false, Some(10L)), // Version newer than the recent
      PartitionState("date=2024-05-08", true, Some(4L)), // Already backfilled
      PartitionState("date=2024-05-09", false, Some(5L)), // Same as current partition
      PartitionState("date=2024-05-10", false, Some(5L)) // Greater than curren
    ).toDS().write.format("delta").save(s"${outputDir}/partitions-state")

    val stateTable = new PartitionsStateTable(sparkSession = sparkSession, baseDir = outputDir)

    stateTable.getPartitionsToBackfill(Map("date=2024-05-05" -> 5L,
      "date=2024-05-06" -> 31L, "date=2024-05-07" -> 5L, "date=2024-05-08" -> 31L, "date=2024-05-09" -> 31L
    ), "date=2024-05-09") shouldEqual Seq("date=2024-05-06")
  }

}