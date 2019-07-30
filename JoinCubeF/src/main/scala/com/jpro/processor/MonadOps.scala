package com.jpro.processor

trait MonadOps[A] {

  def filter(x: Option[A], f: A => Boolean): Option[A] = {
    x match {
      case None => None
      case Some(y) =>
        if (f(y))
          x
        else
          None
    }
  }

  def transform(x: Option[A], f: A => A): Option[A] = {
    x match {
      case None => None
      case Some(y) => Option(f(y))
    }
  }

}
