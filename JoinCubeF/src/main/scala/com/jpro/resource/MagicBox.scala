package com.jpro.resource

import org.apache.logging.log4j.scala.Logging

object MagicBox extends Logging {
  def initializeResource(args: Array[String]): Unit = {
    logger.info("\033[36;1minitialize resource\033[0m")

  }

  def recycleResource(): Unit = {
    logger.info("\033[36;1mrecycle resource\033[0m")

  }
}
