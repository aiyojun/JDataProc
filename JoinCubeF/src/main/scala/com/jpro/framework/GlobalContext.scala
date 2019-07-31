package com.jpro.framework

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.logging.log4j.scala.Logging

import scala.util._

object GlobalContext extends Logging {

  def load(path: String): Properties = {
    Try(ctx.load(new FileInputStream(new File(path)))) match {
      case Failure(_) => logger.error(s"load $path file failed")
      case Success(_) =>
    }
    ctx
  }

  val ctx: Properties = new Properties()

  lazy val MongoURL: String       = ctx.getProperty("mongo.server.url")
  lazy val MongoDB: String        = ctx.getProperty("mongo.database")
  lazy val SysPartCol: String     = ctx.getProperty("dict.sys.part.collection")
  lazy val SysProcessCol: String  = ctx.getProperty("dict.sys.process.collection")
  lazy val SysDefectCol: String   = ctx.getProperty("dict.sys.defect.collection")
  lazy val defectCol: String      = ctx.getProperty("defect.collection")

  lazy val CncColPrefix: String = ctx.getProperty("Cnc.collection.prefix")
//  lazy val Cnc8Col: String = ctx.getProperty("Cnc.cnc8.collection")
//  lazy val Cnc10Col: String = ctx.getProperty("Cnc.cnc10.collection")
//  lazy val CncLaserCol: String = ctx.getProperty("Cnc.laser.collection")

  lazy val partIdKey: String      = ctx.getProperty("sys.part.id.key")
  lazy val jancodeKey: String     = ctx.getProperty("sys.part.jancode.key")
  lazy val upccodeKey: String     = ctx.getProperty("sys.part.upccode.key")
  lazy val cartonVolumeKey: String = ctx.getProperty("sys.part.carton.volume.key")
  lazy val materialTypeKey: String = ctx.getProperty("sys.part.material.type.key")

  lazy val processIdKey: String   = ctx.getProperty("sys.process.id.key")
  lazy val processNameKey: String = ctx.getProperty("sys.process.name.key")

  lazy val defectIdKey: String    = ctx.getProperty("sys.defect.id.key")
  lazy val defectCnKey: String    = ctx.getProperty("sys.defect.cn.key")
  lazy val defectEnKey: String    = ctx.getProperty("sys.defect.en.key")
  lazy val defectTypeKey: String  = ctx.getProperty("sys.defect.type.key")

  lazy val reworkIdOfCnc8: String = ctx.getProperty("rework.id.cnc8")
  lazy val reworkIdOfAno: String = ctx.getProperty("rework.id.ano")
  lazy val reworkIdOfFqc: String = ctx.getProperty("rework.id.fqc")

  lazy val stationNameOfCnc8: String = ctx.getProperty("station.name.cnc8")
  lazy val stationNameOfAno: String = ctx.getProperty("station.name.ano")
  lazy val stationNameOfFqc: String = ctx.getProperty("station.name.fqc")

  lazy val stationIdOfCnc8: String = ctx.getProperty("station.id.cnc8")
  lazy val stationIdOfCnc10: String = ctx.getProperty("station.id.cnc10")
  lazy val stationIdOflaser: String = ctx.getProperty("station.id.laser")

  lazy val stationCollectionPrefix: String = ctx.getProperty("Travel.collection.prefix")
  lazy val defectCollectionPrefix: String  = ctx.getProperty("Defect.collection.prefix")

  lazy val stationExceptionalCol: String = ctx.getProperty("station.exc.collection")

  lazy val ReworkIDMapper: Map[String, String] =
    Map(reworkIdOfCnc8 -> stationNameOfCnc8, reworkIdOfAno -> stationNameOfAno, reworkIdOfFqc -> stationNameOfFqc)

  lazy val DotStationsIDSeq: List[String] =
    List(stationIdOfCnc8, stationIdOfCnc10, stationIdOflaser)

  var working: Boolean = false

  import com.jpro.processor.Sobel._
  lazy val buildsMapping: Map[String, String] =
    JTools.split(ctx.getProperty("build.types")).createMap(s => s.toLowerCase() -> s)
}
