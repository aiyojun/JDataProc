package com.jpro.processor

import org.json4s.JsonAST.JValue

trait Agent
case class Right(opt: JValue) extends Agent
case class Wrong(msg: String) extends Agent

object Agent {
  def apply(jv: JValue): Agent = jv match {
    case null => Wrong("empty element")
    case _ => Right(jv)
  }
}

trait Ops {
  def filter(x: Agent, f: JValue => Boolean): Agent = {
    x match {
      case Wrong(msg) => Wrong(msg)
      case Right(y) =>
        if (f(y))
          x
        else
          Wrong("filter failed")
    }
  }

  def transform(x: Agent, f: JValue => JValue): Agent = {
    x match {
      case Wrong(msg) => Wrong(msg)
      case Right(y) => Right(f(y))
    }
  }
}
