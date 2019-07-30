package com.jpro.framework

import org.apache.logging.log4j.scala.Logging
import org.mongodb.scala._

import com.jpro.framework.GlobalContext._
import com.jpro.util.MongoHelpers._
import com.jpro.processor.Sobel._

case class BasicAttr(cartonVolume: String, meterialType: String, jancode: String, upcode: String)

object MongoProxy extends Logging {
  lazy val uniqueMongo: MongoClient = {
    MongoClient(s"mongodb://$MongoURL")
  }
  lazy val DictSysPart: Map[String, BasicAttr] = {
    implicit class DStr(s: String) {
      def toJ320OrJ420: String = {
        if (s.toLowerCase().contains("j320"))
          "j320"
        else if (s.toLowerCase().contains("j420"))
          "j420"
        else
          ""
      }
    }
    def doc2pair: Document => (String, BasicAttr) = {
      doc => doc.getOrElse("PART_ID","").asString().getValue ->
        BasicAttr(
          doc.getOrElse("CARTON_VOLUME", "").asString().getValue.toJ320OrJ420,
          doc.getOrElse("METERIAL_TYPE", "").asString().getValue,
          doc.getOrElse("JANCODE", "").asString().getValue,
          doc.getOrElse("UPCODE", "").asString().getValue
        )
    }
    uniqueMongo.getDatabase(MongoDB).getCollection(SysPartCol).find().results().createMap[String, BasicAttr](doc2pair)
  }
  lazy val DictSysProcess: Map[String, String] = {
    uniqueMongo.getDatabase(MongoDB).getCollection(SysProcessCol).find()
      .results().createMap[String, String](
        doc => doc.getOrElse("process_id", "").asString().getValue -> doc.getOrElse("process_name", "").asString().getValue
      )
  }
}
