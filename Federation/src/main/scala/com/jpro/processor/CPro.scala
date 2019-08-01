package com.jpro.processor

import com.mongodb.client.model.ReplaceOptions
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.Document
import org.mongodb.scala.model.Filters.equal
import com.jpro.resource.{Context, defe, dote, syse, trae}
import com.jpro.resource.Context.{MongoDB, MongoURL}
import org.apache.logging.log4j.scala.Logging

import scala.util.{Failure, Success, Try}

class CPro extends Logging with Pro {

  private val selfMongo: MongoClient = MongoClient(MongoURL)

  override def trap(s: String): Unit = {
    import com.jpro.storage.MongoHelpers._
    Try(Document(s)) match {
      case Failure(ex) =>
        selfMongo.getDatabase(MongoDB)
          .getCollection[Document](Context.exceptionTable)
          .insertOne(Document("raw" -> s)).results()
      case Success(js) =>
        selfMongo.getDatabase(MongoDB)
          .getCollection[Document](Context.exceptionTable)
          .insertOne(js).results()
    }
  }

  override def proc(raw: JValue): Unit = {
    import com.jpro.storage.MongoProxy._
    val serialNumerValue    = raw | dote.ikUniqueID
    val cellValue           = raw | dote.ikCell
    val machineValue        = raw | dote.ikMachineName
    val processNameValue    = raw | dote.ikStationName

    /// TODO: store first
    import com.jpro.storage.MongoHelpers._
    selfMongo.getDatabase(MongoDB).getCollection[Document](dote.dotTable).insertOne(Document(compact(raw))).results()

    /// TODO: analyze branch
    def updateTravelOp(stationCollection: String): Unit = {
      import com.jpro.storage.MongoHelpers._
      lazy val ncnc = (dote.ikStationName -> processNameValue) ~ (dote.ikMachineName -> machineValue) ~ (dote.ikCell -> cellValue)
      def hasSame(li: List[JValue], p: JValue): Boolean = {
        li.foreach(jv => {
          if (jv \ dote.ikStationName == p \ dote.ikStationName) {
            return true
          }
        })
        false
      }
      val fromTravel = selfMongo.getDatabase(MongoDB)
        .getCollection(stationCollection)
        .find(equal(trae.ikUniqueID, serialNumerValue)).headResult()
      fromTravel match {
        case null =>
        case travel =>
          val jv = parse(travel.toJson())
          jv \ "CNC" match {
            case JNothing =>
              val updated: JValue = jv merge JObject(JField("CNC", JArray(List(ncnc))))
              selfMongo.getDatabase(MongoDB).getCollection[Document](stationCollection)
                .replaceOne(equal(trae.ikUniqueID, serialNumerValue),
                  Document(compact(updated)),
                  new ReplaceOptions().upsert(true))
                .results()
            case JArray(list) =>
              if (!hasSame(list, ncnc)) {
                val updated: JValue = jv merge JObject(JField("CNC", JArray(list :+ ncnc)))
                selfMongo.getDatabase(MongoDB).getCollection[Document](stationCollection)
                  .replaceOne(equal(trae.ikUniqueID, serialNumerValue),
                    Document(compact(updated)),
                    new ReplaceOptions().upsert(true))
                  .results()
              }
            case _ =>
              throw new RuntimeException("CNC type of Travel data")
          }
      }
    }
    /// TODO: query travel data
    val judgeTarget = processNameValue.toLowerCase()
    if (judgeTarget.contains("cnc7") || judgeTarget.contains("cnc8")) {
      updateTravelOp(trae.stationTablePrefix + dote.stationNameOfCnc8.toLowerCase().replace('-', '_'))
      updateTravelOp(trae.stationTablePrefix + dote.stationNameOfCnc10.toLowerCase().replace('-', '_'))
      updateTravelOp(trae.stationTablePrefix + dote.stationNameOfLaser.toLowerCase().replace('-', '_'))
    } else if (judgeTarget.contains("cnc9") || judgeTarget.contains("cnc10")) {
      updateTravelOp(trae.stationTablePrefix + dote.stationNameOfCnc10.toLowerCase().replace('-', '_'))
      updateTravelOp(trae.stationTablePrefix + dote.stationNameOfLaser.toLowerCase().replace('-', '_'))
    }
  }

  implicit class Parasyte(val jv: JValue) {
    def |(k: String): String = jv \ k match {
      case JString(x) => x
      case JNothing   => throw new RuntimeException(s"no such field - $k")
      case _          => throw new RuntimeException(s"invalid field type - $k")
    }

    def join(p: JValue): JValue = jv merge p
  }
}

object CPro {
  def apply(): CPro = new CPro()
}