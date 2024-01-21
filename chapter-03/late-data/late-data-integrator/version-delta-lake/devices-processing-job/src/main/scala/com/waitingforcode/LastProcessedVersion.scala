package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.{File, FileNotFoundException}
import java.nio.charset.Charset

object LastProcessedVersion {

  private val VersionFile = "_last_processed_version"

  def load(baseDir: String): Long = {
    try {
      val versionFromFile = FileUtils.readFileToString(new File(s"${baseDir}/${VersionFile}"), Charset.forName("UTF-8"))
      versionFromFile.toLong
    } catch {
      case _: FileNotFoundException => 0L
    }
  }

  def save(baseDir: String, version: Long) = {
    FileUtils.writeStringToFile(new File(s"${baseDir}/${VersionFile}"), s"${version}", "UTF-8")
  }

}