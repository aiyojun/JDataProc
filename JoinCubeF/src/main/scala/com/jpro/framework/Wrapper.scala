package com.jpro.framework

import com.jpro.processor.MonadOps
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
  implicit def OptionJValueJValueToWrapper(json: Option[JValue]): Wrapper = new Wrapper(json)
}
