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

  lazy val MongoURL: String = ctx.getProperty("mongo.server.url")
  lazy val MongoDB: String = ctx.getProperty("mongo.database")
  lazy val SysPartCol: String = ctx.getProperty("dict.sys.part.collection")
  lazy val SysProcessCol: String = ctx.getProperty("dict.sys.process.collection")
  lazy val SysDefectCol: String = ctx.getProperty("dict.sys.defect.collection")
  lazy val defectCol: String = ctx.getProperty("defect.collection")

  var working: Boolean = false

  import com.jpro.processor.Sobel._
  lazy val buildsMapping: Map[String, String] =
    JTools.split(ctx.getProperty("build.types")).createMap(s => s.toLowerCase() -> s)
}
