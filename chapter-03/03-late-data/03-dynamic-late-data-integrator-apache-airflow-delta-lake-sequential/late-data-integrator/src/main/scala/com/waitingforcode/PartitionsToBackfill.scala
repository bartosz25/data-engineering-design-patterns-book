package com.waitingforcode

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.FileUtils

import java.io.File

object PartitionsToBackfill {

  private val JsonMapper = new ObjectMapper()
  JsonMapper.registerModule(DefaultScalaModule)
  case class BackfillConfiguration(partitions: Seq[Map[String, String]],
                                   lastProcessedVersion: Long)

  def save(outputFilePath: String, backfillConfiguration: BackfillConfiguration): Unit = {
    FileUtils.writeStringToFile(new File(outputFilePath),
      JsonMapper.writeValueAsString(backfillConfiguration), "UTF-8")
  }

}
