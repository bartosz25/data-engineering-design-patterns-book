package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object PartitionsToBackfill {

  case class BackfillConfiguration(partitions: Seq[String])

  def save(outputFilePath: String, backfillConfiguration: BackfillConfiguration): Unit = {
    FileUtils.writeStringToFile(new File(outputFilePath),
      Json.Mapper.writeValueAsString(backfillConfiguration), "UTF-8")
  }

}
