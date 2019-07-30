package com.jpro.framework

import java.util.concurrent.ArrayBlockingQueue

import org.apache.logging.log4j.scala.Logging

case class BaseBlock(head: String, body: String)

class Server extends Logging {

  val sharedQueue = new ArrayBlockingQueue[BaseBlock](
    java.lang.Integer.parseInt(GlobalContext.ctx.getProperty("share.buffer.size")))

  val kafkaProxyOfTra: KafkaProxy = new KafkaProxy(sharedQueue)

//  val dispatcher: Dispatcher = new Dispatcher()

  def prepare(): Server = {
    kafkaProxyOfTra.prepare(GlobalContext.ctx.getProperty("Travel.group.id"),
      GlobalContext.ctx.getProperty("Travel.topic"))
    this
  }

  def start(): Unit = {
    logger.info("=================================")
    logger.info("core service start")
    kafkaProxyOfTra.start()
  }

  def waitThread(): Unit = {
    kafkaProxyOfTra.waitThread()
  }

}

object Server {
  def apply(): Server = new Server()
}