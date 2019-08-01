package com.jpro.resource

import com.jpro.storage.MongoProxy
import org.apache.logging.log4j.scala.Logging

object MagicBox extends Logging {
  def initializeResource(args: Array[String]): Unit = {
    logger.info("\033[36;1minitialize resource\033[0m")

    Context.load("/root/Desktop/JoinCubeF/src/main/resources/library.properties")
    logger.info("Context information as following:")
    Context.props.forEach((k, v) => logger.info(s"    $k: $v"))
    logger.info("All station name:")
    MongoProxy.DictSysProcess.foreach(kv => logger.info(s"    ${kv._2}"))
  }

  def recycleResource(): Unit = {
    Context.working = false
    logger.info("\033[36;1mrecycle resource\033[0m")
  }
}
