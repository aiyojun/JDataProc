package com.jpro.processor

import com.jpro.framework.{GlobalContext, MongoProxy}
import org.apache.logging.log4j.scala.Logging
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.mongodb.scala.{MongoClient, bson}
import com.jpro.framework.GlobalContext._
import com.jpro.framework.MongoProxy._
import org.mongodb.scala.model.Filters.equal

object SobelOfTravel extends Logging {
  val selfMongo: MongoClient = MongoClient(GlobalContext.MongoURL)

  def validate(json: JValue, key: String, seed: Int): Unit = {
    seed match {
      case 0 => json \ key match {
        case JString(_) =>
        case JNothing   => throw new RuntimeException("no such field - " + key)
        case _          => throw new RuntimeException("invalid field type - " + key)
      }
      case 1 => json \ key match {
        case JInt(_)    =>
        case JLong(_)   =>
        case JString(_) =>
        case JNothing   => throw new RuntimeException("no such field - " + key)
        case _          => throw new RuntimeException("invalid field type - " + key)
      }
      case 2 => json \ key match {
        case JInt(_)    =>
        case JNothing   => throw new RuntimeException("no such field - " + key)
        case _          => throw new RuntimeException("invalid field type - " + key)
      }
      case _ => throw new RuntimeException("undefined seed of validate type")
    }
  }

  def process(orig: JValue): JValue = {
    validate(orig, "WORK_ORDER", 0)
    validate(orig, "SERIAL_NUMBER", 0)
    validate(orig, "MODEL_ID", 0)
    validate(orig, "PROCESS_ID", 1)
    validate(orig, "OUT_PROCESS_TIME", 0)
    validate(orig, "RECID", 1)
    validate(orig, "CURRENT_STATUS", 2)
    validate(orig, "WORK_FLAG", 2)
    logger.info("------------------- transform -------------------")
    val serialNumber      = orig \ "SERIAL_NUMBER" match {case JString(s) => s}
    val processIdValue    = orig \ "PROCESS_ID" match {case JString(s) => s}
//    val _id = processIdValue + "_" + serialNumber
    val processValue      = DictSysProcess(processIdValue)
    val stationCol        = "st_" + processValue.toLowerCase.replace('-','_')
    val modelIdValue      = orig \ "MODEL_ID" match {case JString(s) => s}
    val currentStatusValue = orig \ "CURRENT_STATUS" match {case JString(s) => s}
    val workFlagValue     = orig \ "WORK_FLAG" match {case JString(s) => s}
    val recIdValue        = orig \ "RECID" match {case JString(s) => s}

    val defectCol         = "defect_" + processValue.toLowerCase.replace('-','_')
//    val outProcessTimeValue = orig \ "OUT_PROCESS_TIME" match {
//      case JString(s) => s
//    }
    import com.jpro.util.MongoHelpers._
    import org.mongodb.scala.model.Filters._
    val oldRecordSeq = selfMongo.getDatabase(MongoDB).getCollection(stationCol).find(equal("SERIAL_NUMBER", serialNumber)).results()
    val isUpdateOp = oldRecordSeq.count(_ => true) match {
      case 0 => false
      case _ => true
    }

    { /// TODO: G_SN_DEFECT
      val defectDataSeq = selfMongo.getDatabase(MongoDB)
        .getCollection(defectCol)
        .find(equal("RECID", recIdValue))
        .results()
      if (defectDataSeq.nonEmpty) {
        val defectIdValue = defectDataSeq.head.getOrElse("DEFECT_ID", "").asString().getValue
        val defectReasonSeq = selfMongo.getDatabase(MongoDB).getCollection(GlobalContext.SysDefectCol).find(equal("DEFECT_ID", defectIdValue)).results()
        if (defectReasonSeq.nonEmpty) {
          val header = defectReasonSeq.head
          val oneOfReason: JValue =
            ("id" -> defectIdValue) ~
              ("cn" -> header.getOrElse("DEFECT_DESC", "").asString().getValue) ~
              ("en" -> header.getOrElse("DEFECT_DESC2", "").asString().getValue) ~
              ("type" -> header.getOrElse("DEFECT_TYPE", "").asString().getValue)
          /// TODO:
        }
      }
    }


    if (isUpdateOp) {
      val extraAttrOfTime: JValue =
        ("OUT_PROCESS_TIME" -> orig \ "OUT_PROCESS_TIME") ~
          ("first_process_time" -> oldRecordSeq.head.getOrElse("first_process_time", "").asString().getValue)

    } else {
      val extraAttrOfTime: JField = ("first_process_time", orig \ "OUT_PROCESS_TIME")

    }


    val extraAttr: JValue =
      ("PROCESS_NAME" -> DictSysProcess(processIdValue)) ~
        ("COLOR" -> DictSysPart(modelIdValue).upcode) ~
        ("WIFI_4G" -> DictSysPart(modelIdValue).jancode) ~
        ("PRODUCT" -> DictSysPart(modelIdValue).meterialType) ~
        ("PRODUCT_CODE" -> DictSysPart(modelIdValue).cartonVolume)
    processIdValue match {
      case "105399" => // CNC8-WCNC4-QC
      case "600000100" => // CNC10-WCNC5-QC
      case "105405" => // FQC
      case _ =>
    }

    orig
  }

  def proc(raw: JValue): JValue  = {
    // TODO: 1. validate
    validate(raw, "WORK_ORDER", 0)
    validate(raw, "SERIAL_NUMBER", 0)
    validate(raw, "MODEL_ID", 0)
    validate(raw, "PROCESS_ID", 1)
    validate(raw, "OUT_PROCESS_TIME", 0)
    validate(raw, "RECID", 1)
    validate(raw, "CURRENT_STATUS", 2)
    validate(raw, "WORK_FLAG", 2)
    // TODO: 2. parse & extract necessary fields
    val serialNumber      = raw \ "SERIAL_NUMBER" match {case JString(s) => s}
    val processIdValue    = raw \ "PROCESS_ID" match {case JString(s) => s}
    val processValue      = DictSysProcess(processIdValue)
    val stationCol        = "st_" + processValue.toLowerCase.replace('-','_')
    val modelIdValue      = raw \ "MODEL_ID" match {case JString(s) => s}
    val currentStatusValue = raw \ "CURRENT_STATUS" match {case JString(s) => s}
    val workFlagValue     = raw \ "WORK_FLAG" match {case JString(s) => s}
    val recIdValue        = raw \ "RECID" match {case JString(s) => s}
    val defectCol         = "defect_" + processValue.toLowerCase.replace('-','_')
    // TODO: 3. query history data from mongo
    import com.jpro.util.MongoHelpers._
    val history: Option[JValue] = selfMongo.getDatabase(MongoDB).getCollection(stationCol).find(equal("SERIAL_NUMBER", serialNumber)).results()
    // TODO: 4. judge branch
    // TODO: build inner implement
    object InnerImp {
      implicit class Parasyte(val jv: JValue) {
        def pick: JValue = "SERIAL_NUMBER" -> jv \ "SERIAL_NUMBER"
        def join(p: JValue): JValue = {
          jv merge p
        }
        def mix(p: Option[JValue]): JValue = p match {
          case None => jv
          case Some(defect) =>
            val defectEle: JField = JField("defect", JArray(List(defect)))

        }
      }
    }
    history match {
      case None =>
        val part: JValue =
          ("PROCESS_NAME" -> DictSysProcess(processIdValue)) ~
          ("COLOR" -> DictSysPart(modelIdValue).upcode) ~
          ("WIFI_4G" -> DictSysPart(modelIdValue).jancode) ~
          ("PRODUCT" -> DictSysPart(modelIdValue).meterialType) ~
          ("PRODUCT_CODE" -> DictSysPart(modelIdValue).cartonVolume)
        val defect: Option[JValue] = selfMongo.getDatabase(MongoDB).getCollection(defectCol).find(equal("RECID", recIdValue)).results()
        import InnerImp._
        val finner: JValue = raw.pick.join(part).mix(defect)
        finner
      case Some(historyD) =>
        val defect: Option[JValue] = selfMongo.getDatabase(MongoDB).getCollection(defectCol).find(equal("RECID", recIdValue)).results()
        val finner: JValue = historyD.compare(raw).mix(defect)
        finner
    }.store

//    if (historySeq.isEmpty) { // insert branch
//      val part = make
//      val defect: Option[JValue] = selfMongo.getDatabase(MongoDB).getCollection(defectCol).find(equal("DEFECT_ID", recIdValue)).results()
//      raw.pick.merge(part).mix(defect)
//    } else {
//
//    }
    // TODO: 5. kernel processing
    null
  }

  def transform(d: JValue): JValue = {

    null
  }
}
