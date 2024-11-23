package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

case class DataProcessingJobExecutionConfiguration(partition: String, version: Long) {

  def writeAsFile(outputLocation: String): Unit = {
    FileUtils.writeStringToFile(new File(outputLocation),
      Json.Mapper.writeValueAsString(this), "UTF-8")
  }

}

object DataProcessingJobExecutionConfiguration {

  def readFromFile(inputLocation: String): DataProcessingJobExecutionConfiguration = {
    Json.Mapper.readValue(new File(inputLocation), classOf[DataProcessingJobExecutionConfiguration])
  }

  def removeFile(fileToRemove: String) = {
    new File(fileToRemove).delete()
  }

}