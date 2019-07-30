package com.jpro.framework

import java.util.concurrent.ArrayBlockingQueue

import org.apache.logging.log4j.scala.Logging
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util._

class Dispatcher(q: ArrayBlockingQueue[BaseBlock], f: JValue => JValue, g: String => Unit, h: JValue => Unit) extends Thread with Logging {
  val sharedQueue: ArrayBlockingQueue[BaseBlock] = q

  val core: JValue => JValue = f
  val trap: String => Unit = g
  val store: JValue => Unit = h

  override def run(): Unit = {
    while (GlobalContext.working || sharedQueue.size() != 0) {
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
                case Success(njson) =>
                  store(njson)
              }
          }
      }
    }
  }
}
