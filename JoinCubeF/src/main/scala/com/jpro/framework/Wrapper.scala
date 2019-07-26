package com.jpro.framework

import org.json4s.JsonAST.JValue

class Wrapper(json: Option[JValue]) {

  implicit val ops: MonadOps[JValue] = new MonadOps[JValue]{}

  def validate(f: JValue => Boolean): Option[JValue] = {
    ops.filter(json, f)
  }

  def transform(f: JValue => JValue): Option[JValue] = {
    ops.transform(json, f)
  }

}

object Wrapper {
//  implicit def JValueToWrapper(json: JValue) = new Wrapper(json)
  implicit def OptionJValueJValueToWrapper(json: Option[JValue]) = new Wrapper(json)
}
