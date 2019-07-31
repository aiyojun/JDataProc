package com.jpro.processor

import com.jpro.framework._
import com.jpro.framework.GlobalContext._
import com.jpro.framework.MongoProxy._
import org.apache.logging.log4j.scala.Logging
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.mongodb.scala._
import com.mongodb.client.model._
import org.mongodb.scala.bson.Document
import org.mongodb.scala.model.Filters.equal

import scala.util._

class SobelOfTravel extends Logging {
  import SobelOfTravel._
  private val selfMongo: MongoClient = MongoClient(GlobalContext.MongoURL)

  def trap(s: String): Unit = {
    import com.jpro.util.MongoHelpers._
    Try(Document(s)) match {
      case Failure(ex) =>
        selfMongo.getDatabase(MongoDB)
          .getCollection[Document](stationExceptionalCol)
          .insertOne(Document("raw" -> s)).results()
      case Success(js) =>
        selfMongo.getDatabase(MongoDB)
          .getCollection[Document](stationExceptionalCol)
          .insertOne(js).results()
    }
  }

  def proc(raw: JValue): Unit = {
    // TODO: 1. validate & extract necessary fields
    import InnerImp._
    val workOrderValue      = raw | workOrderKey
    val serialNumber        = raw | serialNumberKey
    val modelIdValue        = raw | modelIdKey
    val processIdValue      = raw | processIdKey
    val outProcessTime      = raw | outProcessTimeKey
    val recIdValue          = raw | recIdKey
    val currentStatusValue  = raw | currentStatusKey
    val workFlagValue       = raw | workFlagKey
    val processValue        = DictSysProcess(processIdValue)
    val stationCol          = stationCollectionPrefix + processValue.toLowerCase.replace('-','_')
    val defectCol           = defectCollectionPrefix + processValue.toLowerCase.replace('-','_')
    // TODO: 3. query history data from mongo
    import com.jpro.util.MongoHelpers._
    val history: Option[JValue] =
      selfMongo.getDatabase(MongoDB)
        .getCollection[Document](stationCol)
        .find(equal(serialNumberKey, serialNumber))
        .headResult() match {
          case null => None
          case x => Option(parse(x.toJson()))
        }

    processIdValue match {
      case GlobalContext.reworkIdOfCnc8 | GlobalContext.reworkIdOfAno | GlobalContext.reworkIdOfFqc  =>
        history match {
          case None =>
          case Some(past) =>
            val pastee = past.modify
            Try(
              selfMongo.getDatabase(MongoDB)
                .getCollection[Document](stationCollectionPrefix
                  + DictSysProcess(GlobalContext.ReworkIDMapper(processIdValue))
                    .toLowerCase()
                    .replace('-','_'))
                .replaceOne(
                  equal(serialNumberKey, serialNumber),
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
          (processNameKey -> DictSysProcess(processIdValue)) ~
            (colorKey -> DictSysPart(modelIdValue).upcode) ~
            (wifi4gKey -> DictSysPart(modelIdValue).jancode) ~
            (productKey -> DictSysPart(modelIdValue).meterialType) ~
            (productCodeKey -> DictSysPart(modelIdValue).cartonVolume)
        val defect: Option[JValue] =
          selfMongo.getDatabase(MongoDB)
            .getCollection[Document](defectCol)
            .find(equal(defectRecIdKey, recIdValue))
            .headResult() match {
              case null => None
              case x =>
                implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
                selfMongo.getDatabase(MongoDB)
                  .getCollection[Document](SysDefectCol)
                  .find(equal(GlobalContext.defectIdKey, x.getString(defectIdKey)))
                  .headResult() match {
                    case null => throw new RuntimeException("cannot find defect type in dict")
                    case defectDoc =>
                      val defectDesc = parse(defectDoc.toJson()).extract[DefectType]
                      val _r =
                        (travelDefectIdKey -> defectDesc.DEFECT_ID) ~
                          (travelDefectCnKey -> defectDesc.DEFECT_DESC) ~
                          (travelDefectEnKey -> defectDesc.DEFECT_DESC2) ~
                          (travelDefectTypeKey -> defectDesc.DEFECT_TYPE)
                      Option(_r)
                  }
            }
        val dotc: Option[JValue] = {
          if (!GlobalContext.DotStationsIDSeq.contains(processIdValue)) {
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
            import Sobel._
            val records = selfMongo.getDatabase(MongoDB)
              .getCollection(GlobalContext.CncColPrefix + MongoProxy.DictSysProcess(processIdValue).toLowerCase().replace('-', '_'))
              .find(equal(serialNumberKey, serialNumber)).results()
              .sortWith((p0, p1) => {p0.getObjectId("_id").getTimestamp > p1.getObjectId("_id").getTimestamp})
              .createMap[String, Document](record => record.getString(serialNumberKey) -> record).toJValueList
            Option(JObject(JField("CNC", records)))
          }
        }
        val finner: JValue = raw.pick.join(part).join(dotc).mix(defect)
        finner
      case Some(historyD) =>
        val defect: Option[JValue] =
          selfMongo.getDatabase(MongoDB)
            .getCollection[Document](defectCol)
            .find(equal(defectRecIdKey, recIdValue))
            .headResult() match {
              case null => None
              case x =>
                implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
                selfMongo.getDatabase(MongoDB)
                  .getCollection[Document](SysDefectCol)
                  .find(equal(GlobalContext.defectIdKey, x.getString(defectIdKey)))
                  .headResult() match {
                  case null => throw new RuntimeException("cannot find defect type in dict")
                  case defectDoc =>
                    val defectDesc = parse(defectDoc.toJson()).extract[DefectType]
                    val _r =
                      (travelDefectIdKey -> defectDesc.DEFECT_ID) ~
                        (travelDefectCnKey -> defectDesc.DEFECT_DESC) ~
                        (travelDefectEnKey -> defectDesc.DEFECT_DESC2) ~
                        (travelDefectTypeKey -> defectDesc.DEFECT_TYPE)
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
          equal(serialNumberKey, serialNumber),
          Document(compact(row)),
          new ReplaceOptions().upsert(true)
        )
    ) match {
      case Failure(ex) => logger.error("store final data failed - " + ex)
      case Success(_) =>
    }
  }
}

object SobelOfTravel {
  import GlobalContext._

  private lazy val serialNumberKey  = ctx.getProperty("Travel.unique.id.key")
  private lazy val workOrderKey     = ctx.getProperty("Travel.work.order.key")
  private lazy val modelIdKey       = ctx.getProperty("Travel.model.id.key")
  private lazy val processIdKey     = ctx.getProperty("Travel.process.id.key")
  private lazy val currentStatusKey = ctx.getProperty("Travel.current.status.key")
  private lazy val workFlagKey      = ctx.getProperty("Travel.work.flag.key")
  private lazy val resultKey        = ctx.getProperty("Travel.result.key")
  private lazy val recIdKey         = ctx.getProperty("Travel.rec.id.key")
  private lazy val outProcessTimeKey    = ctx.getProperty("Travel.out.process.time.key")
  private lazy val firstProcessTimeKey  = ctx.getProperty("Travel.first.process.time.key")

  private lazy val defectIdKey      = ctx.getProperty("Defect.defect.id.key")
  private lazy val defectRecIdKey   = ctx.getProperty("Defect.rec.id.key")

  private lazy val buildKey         = ctx.getProperty("Travel.build.key")
  private lazy val siteKey          = ctx.getProperty("Travel.site.key")
  private lazy val siteValue        = ctx.getProperty("Travel.site.value")

  private lazy val travelDefectIdKey = ctx.getProperty("Travel.defect.id.key")
  private lazy val travelDefectEnKey = ctx.getProperty("Travel.defect.en.key")
  private lazy val travelDefectCnKey = ctx.getProperty("Travel.defect.cn.key")
  private lazy val travelDefectTypeKey = ctx.getProperty("Travel.defect.type.key")

  private lazy val travelJsonDefectKey = ctx.getProperty("Travel.json.defect.key")

  private lazy val processNameKey   = ctx.getProperty("Travel.process.name.key")
  private lazy val colorKey         = ctx.getProperty("Travel.color.key")
  private lazy val wifi4gKey        = ctx.getProperty("Travel.wifi.4g.key")
  private lazy val productKey       = ctx.getProperty("Travel.product.key")
  private lazy val productCodeKey   = ctx.getProperty("Travel.product.code.key")

  private lazy val resultStatusFail = ctx.getProperty("Travel.result.value.fail")
  private lazy val resultStatusPass = ctx.getProperty("Travel.result.value.pass")
  private lazy val resultStatusRework = ctx.getProperty("Travel.result.value.rework")
  private lazy val resultStatusMiddle = ctx.getProperty("Travel.result.value.middle")

  def apply(): SobelOfTravel = new SobelOfTravel()

  // TODO: build inner implement
  object InnerImp {
    implicit class Parasyte(val jv: JValue) {

      def modify: JValue = jv merge JObject(JField(resultKey, resultStatusRework))

      private def makeBuild(s: String): JValue = {
        buildsMapping.foreach(kv => {
          if (s.contains(kv._2))
            return kv._2
        })
        throw new RuntimeException(s"cannot find $buildKey in $s")
      }

      def pick: JValue =
        (serialNumberKey -> jv \ serialNumberKey) ~
          (firstProcessTimeKey -> jv \ outProcessTimeKey) ~
          (outProcessTimeKey -> jv \ outProcessTimeKey) ~
          (buildKey -> makeBuild(jv | workOrderKey)) ~
          (siteKey -> siteValue)

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
        if (s0 == resultStatusMiddle && s1 == resultStatusPass) {
          JString(resultStatusRework)
        } else if (s0 == resultStatusMiddle && s1 == resultStatusFail) {
          JString(resultStatusMiddle)
        } else {
          JString(s1)
        }
      }

      def compare(p: JValue): JValue = {
        val jn: JValue =
          (outProcessTimeKey -> p \ outProcessTimeKey) ~
            (resultKey -> buildResult(jv \ resultKey, p \ resultKey))
        jv merge jn
      }

    }
  }
}

sealed case class DefectType(DEFECT_ID: String, DEFECT_DESC: String, DEFECT_DESC2: String, DEFECT_TYPE: String)