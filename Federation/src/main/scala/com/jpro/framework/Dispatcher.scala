package com.jpro.framework

import java.util.concurrent.ArrayBlockingQueue

import com.jpro.processor.Pro
import com.jpro.resource.Context
import com.jpro.util.BaseBlock
import org.apache.logging.log4j.scala.Logging
import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import scala.util._

/**
  * The core working thread
  * You should pass processor
  * of different types to Dispatcher.
  */
class Dispatcher(q: ArrayBlockingQueue[BaseBlock],
                 p: Pro)
  extends Thread with Logging {

  /**
    * One single instance of shared queue
    * for many consumer to process data in it.
    */
  val sharedQueue: ArrayBlockingQueue[BaseBlock] = q

  /**
    * core functor to process data
    */
  val core: JValue => Unit = p.proc

  /**
    * store exceptional data
    */
  val trap: String => Unit = p.trap

  /**
    * Dispatcher loop
    */
  override def run(): Unit = {
    while (Context.working || sharedQueue.size() != 0) {
      Try(sharedQueue.take()) match {
        case Failure(ex) => logger.error("Take data from SharedQueue - " + ex)
        case Success(data) =>
          Try(parse(data.body)) match {
            case Failure(parseEx) =>
              logger.error(parseEx)
              trap(data.body)
            case Success(json) =>
              Try(core(json)) match {
                case Failure(procEx) =>
                  logger.error(procEx)
                  trap(data.body)
                case Success(_) =>
              }
          }
      }
    }
  }
}
