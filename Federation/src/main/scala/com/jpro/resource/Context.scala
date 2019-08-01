package com.jpro.resource

import java.io.{File, FileInputStream}
import java.util.Properties

import com.jpro.util.JTools
import org.apache.logging.log4j.scala.Logging

import scala.util.{Failure, Success, Try}

object Context extends Logging { self =>

  def load(path: String): Properties = {
    Try(props.load(new FileInputStream(new File(path)))) match {
      case Failure(_) => logger.error(s"load $path file failed")
      case Success(_) =>
    }
    props
  }

  val props: Properties = new Properties()

  def | (k: String): String = props.getProperty(k)

  lazy val MongoDB  : String = self | "mongo.database"
  lazy val MongoURL : String = self | "mongo.url"

  lazy val exceptionTable: String = self | "exception.table"

  var working: Boolean = false

  import com.jpro.util.Sobel._
  lazy val buildsMapping: Map[String, String] =
    JTools.split(Context | "build.types").createMap(s => s.toLowerCase() -> s)

}