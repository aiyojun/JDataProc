package com.jpro.framework

import com.jpro.processor.{Agent, Ops, Right}
import org.json4s.JsonAST.JValue

class Wrapper2(json: Agent) {
  implicit val ops: Ops = new Ops {}

  def validate(f: JValue => Boolean): Agent = {
    ops.filter(json, f)
  }

  def transform(f: JValue => JValue): Agent = {
    ops.transform(json, f)
  }


}

object Wrapper2 {
  implicit def AgentJValueJValueToWrapper(json: Right): Wrapper2 = new Wrapper2(json)
}
