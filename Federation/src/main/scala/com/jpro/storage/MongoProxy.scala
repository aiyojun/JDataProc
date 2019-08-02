package com.jpro.storage

import com.jpro.resource.Context.{MongoURL, MongoDB}
import com.jpro.util.Sobel._
import MongoHelpers._
import org.apache.logging.log4j.scala.Logging
import org.mongodb.scala._
import com.jpro.resource.{defe, dote, syse, trae}


object MongoProxy extends Logging {

  case class BasicAttr(cartonVolume: String, meterialType: String, jancode: String, upcode: String)

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
      doc => doc.getString(syse.skPartID) ->
        BasicAttr(
          doc.getString(syse.skPartProductCode).toJ320OrJ420,
          doc.getString(syse.skPartProduct),
          doc.getString(syse.skPartColor),
          doc.getString(syse.skPartMedia)
        )
    }
    uniqueMongo.getDatabase(MongoDB).getCollection(syse.extraTable).find().results().createMap[String, BasicAttr](doc2pair)
  }

  lazy val DictSysProcess: Map[String, String] = {
    uniqueMongo.getDatabase(MongoDB).getCollection(syse.processTable).find()
      .results() match {
      case Nil | null =>
        logger.warn(s"no process in ${syse.processTable}")
        null
      case seq =>
        seq.createMap[String, String](
          doc => doc.getString(syse.skProcessID) -> doc.getString(syse.skProcessName)
        )
    }
  }
}
