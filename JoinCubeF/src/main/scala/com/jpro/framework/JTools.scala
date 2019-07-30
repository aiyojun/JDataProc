package com.jpro.framework

object JTools {
  def split(rest: String, assemble: String = "", isOpened: Boolean = false): List[String] = {
    if (rest.length == 0)
      return Nil
    val c = rest.charAt(0)
    if (c == '"' && !isOpened) {
      split(rest.substring(1, rest.length), isOpened = true)
    } else if (c == '"' && isOpened) {
      if (!assemble.isEmpty) {
        assemble :: split(rest.substring(1, rest.length))
      } else {
        split(rest.substring(1, rest.length))
      }
    } else {
      if (isOpened) {
        split(rest.substring(1, rest.length), assemble + c, isOpened = true)
      } else {
        split(rest.substring(1, rest.length))
      }
    }
  }
}
