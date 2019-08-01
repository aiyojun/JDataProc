package com.jpro.framework

import java.util.concurrent.ArrayBlockingQueue

import com.jpro.processor.{APro, CPro, DPro}
import com.jpro.resource.Context
import com.jpro.util.BaseBlock
import org.apache.logging.log4j.scala.Logging

class Server extends Logging {

  lazy val sharedQueueOfTrav = new ArrayBlockingQueue[BaseBlock](
    java.lang.Integer.parseInt(Context | "share.buffer.size"))
  lazy val sharedQueueOfCnc = new ArrayBlockingQueue[BaseBlock](
    java.lang.Integer.parseInt(Context | "share.buffer.size"))
  lazy val sharedQueueOfDefe = new ArrayBlockingQueue[BaseBlock](
    java.lang.Integer.parseInt(Context | "share.buffer.size"))

  val kafkaProxyOfTra: KafkaProxy = new KafkaProxy(sharedQueueOfTrav)
  val kafkaProxyOfCnc: KafkaProxy = new KafkaProxy(sharedQueueOfCnc)
  val kafkaProxyOfDef: KafkaProxy = new KafkaProxy(sharedQueueOfDefe)

  lazy val dispatchersOfTrav: List[Dispatcher] =
    List.fill(Integer.parseInt(Context | "thread.size.travel"))(new Dispatcher(sharedQueueOfTrav, APro()))
  lazy val dispatchersOfCnc: List[Dispatcher] =
    List.fill(Integer.parseInt(Context | "thread.size.cnc"))(new Dispatcher(sharedQueueOfCnc, CPro()))
  lazy val dispatchersOfDefe: List[Dispatcher] =
    List.fill(Integer.parseInt(Context | "thread.size.defect"))(new Dispatcher(sharedQueueOfDefe, DPro()))

  def prepare(): Server = {
    kafkaProxyOfTra.prepare(Context | "kafka.travel.group.id",
      Context | "kafka.travel.topic")
    kafkaProxyOfCnc.prepare(Context | "kafka.cnc.group.id",
      Context | "kafka.cnc.topic")
    kafkaProxyOfDef.prepare(Context | "kafka.defect.group.id",
      Context | "kafka.defect.topic")
    this
  }

  def start(): Unit = {
    logger.info("=================================")
    logger.info("core service start")
    kafkaProxyOfTra.start()
    kafkaProxyOfCnc.start()
    kafkaProxyOfDef.start()
    dispatchersOfTrav.foreach(_.start)
    dispatchersOfDefe.foreach(_.start)
    dispatchersOfCnc.foreach(_.start)
  }

  def waitThread(): Unit = {
    kafkaProxyOfTra.waitThread()
    kafkaProxyOfCnc.waitThread()
    kafkaProxyOfDef.waitThread()
  }

}

object Server {
  def apply(): Server = new Server()
}