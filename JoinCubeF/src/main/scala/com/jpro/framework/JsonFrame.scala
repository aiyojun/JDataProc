package com.jpro.framework

import org.apache.logging.log4j.scala.Logging
import org.json4s._
import org.json4s.jackson.JsonMethods._

class JsonFrame(j: JValue) extends Logging {
//  val json: JValue = j
//
//  def validate(f: JValue => Boolean): Option[JsonFrame] = if (f(json)) Option(this) else Option(null)
//
//  def filter(f: JValue => Boolean): Option[JsonFrame] = if (f(json)) Option(this) else Option(null)
//
//  override def toString: String = compact(json)
}

object JsonFrame {
//  def apply(s: String): JsonFrame = new JsonFrame(s)
}