name := "backfilling_configurator"
organization := "com.waitingforcode"
version := "1.0.0-SNAPSHOT"


val sparkVersion = "3.5.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test

assemblyMergeStrategy in assembly := {
  // The META-INF/services/org.apache.spark.sql.sources.DataSourceRegister must be in the JAR
  case PathList("META-INF", "services", x @ _*) => MergeStrategy.concat
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.last
}