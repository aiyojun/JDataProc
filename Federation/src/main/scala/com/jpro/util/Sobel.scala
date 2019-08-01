package com.jpro.util

object Sobel {
  implicit class MapCreator[A](val seq: Seq[A]) extends ImplicitMapper[A]

  trait ImplicitMapper[A] {
    val seq: Seq[A]
    def createMap[B, C](f: A => (B, C))(implicit li: Seq[A] = seq): Map[B, C] = {
      if (li.isEmpty) {
        Map()
      } else {
        Map(f(li.head)) ++ createMap(f)(li.drop(1))
      }
    }
  }
}
