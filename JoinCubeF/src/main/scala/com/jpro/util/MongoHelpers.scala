package com.jpro.util

import java.util.concurrent.TimeUnit

import org.mongodb.scala.Observable
import org.mongodb.scala.bson.Document

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object MongoHelpers {
  implicit class DocumentObservable(val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: Document => String = doc => doc.toJson()
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: C => String
    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
  }
}
