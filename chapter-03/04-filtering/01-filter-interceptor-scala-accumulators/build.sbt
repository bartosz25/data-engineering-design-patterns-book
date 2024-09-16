name := "filter_interceptor"
organization := "com.waitingforcode"
version := "1.0.0-SNAPSHOT"


val sparkVersion = "3.5.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
