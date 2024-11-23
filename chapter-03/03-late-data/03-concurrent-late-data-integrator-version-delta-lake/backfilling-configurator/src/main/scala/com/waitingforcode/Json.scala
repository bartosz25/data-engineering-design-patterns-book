package com.waitingforcode

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object Json {

  val Mapper = new ObjectMapper()
  Mapper.registerModule(DefaultScalaModule)
}
