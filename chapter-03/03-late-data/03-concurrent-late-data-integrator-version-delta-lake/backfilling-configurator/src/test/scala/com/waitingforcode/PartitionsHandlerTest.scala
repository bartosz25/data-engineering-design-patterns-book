package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class PartitionsHandlerTest extends AnyFlatSpec with Matchers with SparkSessionSpec {

  behavior of "partitions handler"

  it should "return last version for each partition after updating only one partition in the end" in {
    val outputDir = "/tmp/test_partitions_handler_1"
    FileUtils.deleteDirectory(new File(outputDir))
    import sparkSession.implicits._
    val dataset = Seq(
      ("A", "a", 1), ("A", "a", 3), ("A", "a", 5),
      ("B", "b", 6), ("B", "b", 4), ("B", "b", 2)
    ).toDF("letter", "lower", "value")
    dataset.write.mode(SaveMode.Overwrite).partitionBy("letter", "lower").format("delta").save(outputDir)

    val handler = new PartitionsHandler(sparkSession = sparkSession, tablePath = outputDir)

    handler.getLastVersionForEachPartition shouldEqual Map("letter=A/lower=a" -> 0, "letter=B/lower=b" -> 0)

    val datasetToUpdateFully = Seq(
      ("A", "a", 1), ("A", "a", 3), ("A", "a", 5),
      ("B", "b", 6), ("B", "b", 4), ("B", "b", 2)
    ).toDF("letter", "lower", "value")
    datasetToUpdateFully.write.mode(SaveMode.Append).partitionBy("letter", "lower").format("delta").save(outputDir)

    handler.getLastVersionForEachPartition shouldEqual Map("letter=A/lower=a" -> 1, "letter=B/lower=b" -> 1)

    val datasetToUpdatePartially = Seq(
      ("B", "b", 6), ("B", "b", 4), ("B", "b", 2)
    ).toDF("letter", "lower", "value")
    datasetToUpdatePartially.write.mode(SaveMode.Append).partitionBy("letter", "lower").format("delta").save(outputDir)

    handler.getLastVersionForEachPartition shouldEqual Map("letter=A/lower=a" -> 1, "letter=B/lower=b" -> 2)
  }

  it should "keep the snapshot version unchanged for each instance despite new data added" in {

    val outputDir = "/tmp/test_partitions_handler_2"
    FileUtils.deleteDirectory(new File(outputDir))
    import sparkSession.implicits._
    val dataset = Seq(
      ("A", "a", 1), ("A", "a", 3), ("A", "a", 5),
      ("B", "b", 6), ("B", "b", 4), ("B", "b", 2)
    ).toDF("letter", "lower", "value")
    dataset.write.mode(SaveMode.Overwrite).partitionBy("letter", "lower").format("delta").save(outputDir)

    val handler = new PartitionsHandler(sparkSession = sparkSession, tablePath = outputDir)

    handler.lastVersionOfTheTable shouldEqual 0

    dataset.write.mode(SaveMode.Append).partitionBy("letter", "lower").format("delta").save(outputDir)
    handler.lastVersionOfTheTable shouldEqual 0

    val handlerNew = new PartitionsHandler(sparkSession = sparkSession, tablePath = outputDir)
    handlerNew.lastVersionOfTheTable shouldEqual 1
  }

  it should "ignore version that don't add new data" in {
    val outputDir = "/tmp/test_partitions_handler_3"
    FileUtils.deleteDirectory(new File(outputDir))
    import sparkSession.implicits._
    val dataset = Seq(
      ("A", "a", 1), ("A", "a", 3), ("A", "a", 5),
      ("B", "b", 6), ("B", "b", 4), ("B", "b", 2)
    ).toDF("letter", "lower", "value")
    dataset.write.mode(SaveMode.Overwrite).partitionBy("letter", "lower").format("delta").save(outputDir)

    dataset.write.mode(SaveMode.Append).partitionBy("letter", "lower").format("delta").save(outputDir)
    sparkSession.read.format("delta").load(outputDir).repartition(1)
      .write.option("dataChange", "false").format("delta").mode("overwrite")
      .save(outputDir)

    val handler = new PartitionsHandler(sparkSession = sparkSession, tablePath = outputDir)

    handler.getLastVersionForEachPartition shouldEqual Map("letter=A/lower=a" -> 1, "letter=B/lower=b" -> 1)
    handler.lastVersionOfTheTable shouldEqual 2
  }
}
