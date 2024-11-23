package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.charset.Charset
import java.nio.file.NoSuchFileException

object LastProcessedVersion {

  private val VersionFile = "_last_processed_version"

  def load(baseDir: String): Long = {
    try {
      val versionFromFile = FileUtils.readFileToString(new File(s"${baseDir}/${VersionFile}"), Charset.forName("UTF-8"))
      versionFromFile.toLong
    } catch {
      case _: NoSuchFileException => 0L
    }
  }

}