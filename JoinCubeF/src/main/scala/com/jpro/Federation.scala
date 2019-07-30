package com.jpro

import java.util.concurrent.TimeUnit

import com.jpro.framework.MongoProxy.logger
import com.jpro.framework.{BasicAttr, GlobalContext, JTools, Server}
import com.jpro.resource.MagicBox
import org.apache.logging.log4j.scala.Logging
import org.json4s.JsonAST.JValue
import org.mongodb.scala.{Document, MongoClient}
import sun.misc.Signal

import scala.concurrent.duration.Duration
import scala.util._

object Federation extends Logging {
  def main(args: Array[String]): Unit = {
    test()
    System.exit(2)

    println(
      " ____  ____  ____  ____  ____    __   ____  ____  _____  _  _ \n" +
      "( ___)( ___)(  _ \\( ___)(  _ \\  /__\\ (_  _)(_  _)(  _  )( \\( )\n" +
      " )__)  )__)  )(_) ))__)  )   / /(__)\\  )(   _)(_  )(_)(  )  ( \n" +
      "(__)  (____)(____/(____)(_)\\_)(__)(__)(__) (____)(_____)(_)\\_)\n"
    )
    logger.info("Federation start ...")

    MagicBox.initializeResource(args)

    Signal.handle(new Signal("INT"), _ => {
      logger.info("program received [ INT ] signal")
      MagicBox.recycleResource()
    })

    /// TODO: launch main program instance
    GlobalContext.working = true
    val server = Server()
    server.prepare().start()

//    MagicBox.recycleResource()
    server.waitThread()
    logger.info("\033[36;1mrecycle complete, program exit.\033[0m")
  }

  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  def test(): Unit = {
    import org.mongodb.scala.model.Filters._
    import scala.concurrent.Await
    import scala.concurrent._
    import scala.concurrent.ExecutionContext.Implicits.global
//    import com.jpro.util.MongoHelpers._
//    import com.jpro.processor.Sobel._
////    val observable =
//    def f: Document => (String, BasicAttr) = {
//      doc => doc.getOrElse("PART_ID","").asString().getValue ->
//        BasicAttr(
//          doc.getOrElse("CARTON_VOLUME", "").asString().getValue,
//          doc.getOrElse("METERIAL_TYPE", "").asString().getValue,
//          doc.getOrElse("JANCODE", "").asString().getValue,
//          doc.getOrElse("UPCODE", "").asString().getValue
//        )
//    }
//    MongoClient("mongodb://172.16.1.244:27017")
//      .getDatabase("wonder")
////      .getCollection("sys_part")
//      .getCollection("sys_process")
//      .find()
////      .find(equal("PART_ID", "1000004973"))
//      .results().createMap[String, String](
//      doc => doc.getOrElse("process_name", "").asString().getValue.toLowerCase() -> doc.getOrElse("process_id", "").asString().getValue
//    ).foreach(kv => println(s"${kv._1} \t ${kv._2}"))
//      .results().createMap(f).foreach(p => println(s"${p._1}\t: ${p._2.cartonVolume} \t; ${p._2.meterialType} \t; ${p._2.jancode} \t; ${p._2.upcode}"))
//    Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS)).foreach(every => println(every.toJson()))
//      .find(equal("PART_ID", "1000004973")).toFuture().onComplete({case Success(value) => println(value.foreach(_.toJson()))})
//      .foreach(every => println(every.toJson()))
    println("----")

////    val json: JValue = parse("{\"name\":\"xiaoming\",\"age\":12,\"hobby\":[\"swimming\",\"football\"]}")
//    val json: JValue = parse("{\"WORKORDER\":\"StanfordCB-Ramp-180628\",\"SERIAL_NUMBER\":\"RSH0000071550172\",\"MODEL_ID\":\"6000000114\",\"PROCESS_ID\":105405,\"OUT_PROCESS_TIME\":\"08-5月 -19 12.56.57.000000 下午\",\"RECID\":55499705527,\"CURRENT_STATUS\":1,\"WORK_FLAG\":0}")
//    val njson: JValue = parse("{\"WORK_ORDER\":43}")
//    val n2json = json merge njson
//    import com.jpro.framework.Wrapper2.AgentJValueJValueToWrapper
////    import com.jpro.framework.SobelOfTravel.validate
////    val res = Right(json).validate(validate)
////    val res = Option(json).validate(validate)
////    println("----: ")
////    Try(SobelOfTravel.process(json)) match {
////      case Failure(exception) => println(exception)
////      case Success(value) => println(compact(value))
////    }
////    println(compact(n2json))
////    println(json \ "name")
//    println("begin")
//    def createMap(li: List[String]): Map[String, String] = {
//      if (li.isEmpty) {
//        Map()
//      } else {
//        createMap(li.drop(1)) + (li.head.toLowerCase -> li.head)
//      }
//    }
//    val li = JTools.split("[\"Ramp\", \"DOE\", \"DR2\", \"DVT\", \"EVT\", \"P0\", \"P1\", \"P2\",\"Pre-EVT\"]")
//    li.foreach(t => print(s"$t "))
//    createMap(li).foreach(p => println(s"  ${p._1} \t- ${p._2}"))
//    println("\nover")
//    implicit class DDStr(s: String) {
//      def toJ320OrJ420: String = {
//        if (s.toLowerCase().contains("j320"))
//          "j320"
//        else if (s.toLowerCase().contains("j420"))
//          "j420"
//        else
//          ""
//      }
//    }
//    println("::: " + "J320-Housing".toJ320OrJ420)
    println("station_" +"2D-BC-QC".toLowerCase.replace('-','_'))
  }
}
