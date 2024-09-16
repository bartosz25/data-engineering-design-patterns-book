package com.waitingforcode

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

class SparkSessionFactory(outputLocation: String) {


  val derbyLocation = s"${outputLocation}/derby/"

  val sparkSession: SparkSession = {
    SparkSession.builder()
      .config("spark.sql.session.timeZone", "UTC")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", s"${outputLocation}/spark/warehouse")
      .enableHiveSupport()
      .getOrCreate()
  }

}

object SparkSessionFactoryInitializer {

  def init(outputLocation: String): SparkSessionFactory = {
    val sparkSessionFactory = new SparkSessionFactory(outputLocation)
    System.setProperty("derby.system.home", sparkSessionFactory.derbyLocation)
    sparkSessionFactory
  }

}