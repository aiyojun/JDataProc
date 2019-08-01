package com.jpro.processor

import org.json4s.JsonAST.JValue

trait Pro {
  def trap(s: String): Unit
  def proc(raw: JValue): Unit
}
