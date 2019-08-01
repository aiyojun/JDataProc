package com.jpro.processor

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.mongodb.scala.MongoClient
import com.jpro.resource._
import com.jpro.storage.MongoProxy.DictSysProcess
import org.json4s.jackson.JsonMethods.parse
import org.mongodb.scala.model.Filters.equal
import com.jpro.storage.MongoHelpers._
import com.jpro.util.DefectType
import com.mongodb.client.model.ReplaceOptions
import org.mongodb.scala.bson.Document
import com.jpro.resource.{defe, dote, syse, trae}
import com.jpro.resource.Context.{MongoDB, MongoURL}
import org.apache.logging.log4j.scala.Logging

import scala.util.{Failure, Success, Try}

class DPro extends Logging with Pro {
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

  private val selfMongo: MongoClient = MongoClient(MongoURL)

  override def proc(raw: JValue): Unit = {
    val recIdValue          = raw | defe.ikFailID
    val processIdValue      = raw | defe.ikStationID
    val defectIdValue       = raw | defe.ikDefectID
    val serialNumberValue   = raw | defe.ikUniqueID

    val defec: JValue = selfMongo.getDatabase(MongoDB)
      .getCollection[Document](syse.defectTable)
      .find(equal(syse.skDefectID, defectIdValue))
      .headResult() match {
        case null => throw new RuntimeException("cannot find defect type in dict")
        case sysDoc =>
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          val defectDesc = parse(sysDoc.toJson()).extract[DefectType]
          (defe.okDefectID -> defectDesc.DEFECT_ID) ~
            (defe.okDefectCn -> defectDesc.DEFECT_DESC) ~
            (defe.okDefectEn -> defectDesc.DEFECT_DESC2) ~
            (defe.okDefectType-> defectDesc.DEFECT_TYPE)
      }

    val stationCollection = trae.stationTablePrefix + DictSysProcess(processIdValue).toLowerCase().replace('-', '_')
    /// TODO: store anyhow
    selfMongo.getDatabase(MongoDB).getCollection[Document](defe.defectTable).insertOne(Document(compact(defec))).results()

    /// TODO: update travel data
    val tr: Option[JValue] = selfMongo.getDatabase(MongoDB)
      .getCollection[Document](stationCollection)
      .find(equal(trae.ikUniqueID, serialNumberValue))
      .headResult() match {
        case null => None
        case x => Option(parse(x.toJson()))
      }
    tr match {
      case None =>
      case Some(target) =>
        target.update(defec)
        /// TODO: store/update travel
        selfMongo.getDatabase(MongoDB)
          .getCollection[Document](stationCollection)
          .replaceOne(equal(trae.ikUniqueID, serialNumberValue),
            Document(compact(target)),
            new ReplaceOptions().upsert(true))
          .results()
    }
  }

  implicit class Parasyte(val jv: JValue) {
    def |(k: String): String = jv \ k match {
      case JString(x) => x
      case JNothing   => throw new RuntimeException(s"no such field - $k")
      case _          => throw new RuntimeException(s"invalid field type - $k")
    }

    def join(p: JValue): JValue = jv merge p

    def update(p: JValue): JValue = jv \ trae.okGenDefectKey match {
      case JNothing => jv merge JObject(JField(trae.okGenDefectKey, JArray(List(p))))
      case JArray(list) => jv merge JObject(JField(trae.okGenDefectKey, JArray(list :+ p)))
    }
  }
}

object DPro {
  def apply(): DPro = new DPro()
}