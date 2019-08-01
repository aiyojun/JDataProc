package com.jpro.processor

import com.jpro.framework._
import com.jpro.resource.{Context, defe, dote, syse, trae}
import com.jpro.storage.MongoProxy._
import com.jpro.util.DefectType
import org.apache.logging.log4j.scala.Logging
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.mongodb.scala._
import com.mongodb.client.model._
import org.mongodb.scala.bson.Document
import org.mongodb.scala.model.Filters.equal
import com.jpro.resource.Context.{MongoDB, MongoURL}

import scala.util._

class APro extends Logging with Pro {
  import APro._
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
    // TODO: 1. validate & extract necessary fields
    import InnerImp._
    val workOrderValue      = raw | trae.ikCompose
    val serialNumber        = raw | trae.ikUniqueID
    val modelIdValue        = raw | trae.ikExtraID
    val processIdValue      = raw | trae.ikStationID
    val outProcessTime      = raw | trae.ikOutTime
    val recIdValue          = raw | trae.ikFailID
    val currentStatusValue  = raw | trae.ikStateOne
    val workFlagValue       = raw | trae.ikStateTwo
    val processValue        = DictSysProcess(processIdValue)
    val stationCol          = trae.stationTablePrefix + processValue.toLowerCase.replace('-','_')
    // TODO: 3. query history data from mongo
    import com.jpro.storage.MongoHelpers._
    val history: Option[JValue] =
      selfMongo.getDatabase(MongoDB)
        .getCollection[Document](stationCol)
        .find(equal(trae.ikUniqueID, serialNumber))
        .headResult() match {
        case null => None
        case x => Option(parse(x.toJson()))
      }

    processIdValue match {
      case trae.reworkStationIDOfCnc8 | trae.reworkStationIDOfAno | trae.reworkStationIDOfFqc  =>
        history match {
          case None =>
          case Some(past) =>
            val pastee = past.modify
            Try(
              selfMongo.getDatabase(MongoDB)
                .getCollection[Document](trae.stationTablePrefix
                + DictSysProcess(trae.ReworkIDMapper(processIdValue))
                .toLowerCase()
                .replace('-','_'))
                .replaceOne(
                  equal(trae.ikUniqueID, serialNumber),
                  Document(compact(pastee)),
                  new ReplaceOptions().upsert(true)
                )
            ) match {
              case Failure(ex) => logger.error("store final data failed - " + ex)
              case Success(_) =>
            }
        }
        return
      case _ =>
    }
    // TODO: 4. judge branch
    import InnerImp._
    val row = history match {
      case None =>
        val part: JValue =
          (trae.okGenStation -> DictSysProcess(processIdValue)) ~
            (trae.okGenColor -> DictSysPart(modelIdValue).upcode) ~
            (trae.okGenMedia -> DictSysPart(modelIdValue).jancode) ~
            (trae.okGenProduct -> DictSysPart(modelIdValue).meterialType) ~
            (trae.okGenProductID -> DictSysPart(modelIdValue).cartonVolume)
        val defect: Option[JValue] =
          selfMongo.getDatabase(MongoDB)
            .getCollection[Document](com.jpro.resource.defe.defectTable)
            .find(equal(defe.ikFailID, recIdValue))
            .headResult() match {
            case null => None
            case x =>
              implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
              selfMongo.getDatabase(MongoDB)
                .getCollection[Document](syse.defectTable)
                .find(equal(syse.skDefectID, x.getString(syse.skDefectID)))
                .headResult() match {
                case null => throw new RuntimeException("cannot find defect type in dict")
                case defectDoc =>
                  val defectDesc = parse(defectDoc.toJson()).extract[DefectType]
                  val _r =
                    (defe.okDefectID -> defectDesc.DEFECT_ID) ~
                      (defe.okDefectCn -> defectDesc.DEFECT_DESC) ~
                      (defe.okDefectEn -> defectDesc.DEFECT_DESC2) ~
                      (defe.okDefectType -> defectDesc.DEFECT_TYPE)
                  Option(_r)
              }
          }
        val dotc: Option[JValue] = {
          if (!trae.DotStationsIDSeq.contains(processIdValue)) {
            None
          } else {
            implicit class Convertor(val m: Map[String, Document]) {
              def toJValueList(implicit tm: Map[String, Document] = m): List[JValue] = {
                if (tm.isEmpty)
                  Nil
                else
                  parse(tm.head._2.toJson()) :: toJValueList(tm.drop(1))
              }
            }
            import com.jpro.util.Sobel._
            val records = selfMongo.getDatabase(MongoDB)
              .getCollection(dote.dotTable)
              .find(equal(dote.ikUniqueID, serialNumber)).results()
              .sortWith((p0, p1) => {p0.getObjectId("_id").getTimestamp > p1.getObjectId("_id").getTimestamp})
              .createMap[String, Document](record => record.getString(dote.ikUniqueID) -> record).toJValueList
            Option(JObject(JField("CNC", records)))
          }
        }
        val finner: JValue = raw.pick.join(part).join(dotc).mix(defect)
        finner
      case Some(historyD) =>
        val defect: Option[JValue] =
          selfMongo.getDatabase(MongoDB)
            .getCollection[Document](defe.defectTable)
            .find(equal(defe.ikFailID, recIdValue))
            .headResult() match {
            case null => None
            case x =>
              implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
              selfMongo.getDatabase(MongoDB)
                .getCollection[Document](syse.defectTable)
                .find(equal(syse.skDefectID, x.getString(syse.skDefectID)))
                .headResult() match {
                case null => throw new RuntimeException("cannot find defect type in dict")
                case defectDoc =>
                  val defectDesc = parse(defectDoc.toJson()).extract[DefectType]
                  val _r =
                    (defe.okDefectID -> defectDesc.DEFECT_ID) ~
                      (defe.okDefectCn -> defectDesc.DEFECT_DESC) ~
                      (defe.okDefectEn -> defectDesc.DEFECT_DESC2) ~
                      (defe.okDefectType -> defectDesc.DEFECT_TYPE)
                  Option(_r)
              }
          }
        val finner: JValue = historyD.compare(raw).mix(defect)
        finner
    }
    /// TODO: final starage
    Try(
      selfMongo.getDatabase(MongoDB)
        .getCollection[Document](stationCol)
        .replaceOne(
          equal(trae.ikUniqueID, serialNumber),
          Document(compact(row)),
          new ReplaceOptions().upsert(true)
        ).results()
    ) match {
      case Failure(ex) => logger.error("store final data failed - " + ex)
      case Success(_) =>
    }
  }
}

object APro {
  private lazy val travelJsonDefectKey = trae.okGenDefectKey

  def apply(): APro = new APro()

  // TODO: build inner implement
  object InnerImp {
    implicit class Parasyte(val jv: JValue) {

      def modify: JValue = jv merge JObject(JField(trae.okGenState, trae.ovGenRework))

      private def makeBuild(s: String): JValue = {
        Context.buildsMapping.foreach(kv => {
          if (s.contains(kv._2))
            return kv._2
        })
        throw new RuntimeException(s"cannot find ${trae.okGenCompose} in $s")
      }

      def pick: JValue =
        (trae.ikUniqueID -> jv \ trae.ikUniqueID) ~
          (trae.okGenrstTime -> jv \ trae.ikOutTime) ~
          (trae.ikOutTime -> jv \ trae.ikOutTime) ~
          (trae.okGenCompose -> makeBuild(jv | trae.ikCompose)) ~
          (trae.okGenAuto -> trae.ovGenAuto)

      def |(k: String): String = jv \ k match {
        case JString(x) => x
        case JNothing   => throw new RuntimeException(s"no such field - $k")
        case _          => throw new RuntimeException(s"invalid field type - $k")
      }

      def join(p: JValue): JValue = jv merge p

      def join(p: Option[JValue]): JValue = p match {
        case None => jv
        case Some(x) => jv merge x
      }

      def mix(p: Option[JValue]): JValue = jv \ travelJsonDefectKey match {
        case JNothing => p match {
          case None => jv
          case Some(defect) => JObject(JField(travelJsonDefectKey, JArray(List(defect)))) merge jv
        }
        case JArray(arr) => p match {
          case None => jv
          case Some(defect) => JObject(JField(travelJsonDefectKey, JArray(arr :+ defect))) merge jv
        }
        case _ => jv
      }

      private def buildResult(p0: JValue, p1: JValue): JValue = {
        val s0 = p0 match {case JString(s) => s; case _ => throw new RuntimeException("invalid Travel result field")}
        val s1 = p1 match {case JString(s) => s; case _ => throw new RuntimeException("invalid Travel result field")}
        if (s0 == trae.ovGenMiddle && s1 == trae.ovGenPass) {
          JString(trae.ovGenRework)
        } else if (s0 == trae.ovGenMiddle && s1 == trae.ovGenFail) {
          JString(trae.ovGenMiddle)
        } else {
          JString(s1)
        }
      }

      def compare(p: JValue): JValue = {
        val jn: JValue =
          (trae.ikOutTime -> p \ trae.ikOutTime) ~
            (trae.okGenState -> buildResult(jv \ trae.okGenState, p \ trae.okGenState))
        jv merge jn
      }

    }
  }
}