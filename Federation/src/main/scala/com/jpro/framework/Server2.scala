package com.jpro.framework

import java.util
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.jpro.resource.Context
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.logging.log4j.scala.Logging
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.jackson.JsonMethods.parse
import org.mongodb.scala.bson.collection.immutable.Document

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Server2(val topic: String,
              val groupId: String,
              val core: JValue => Unit,
              val trap: String => Unit)
  extends Thread with Logging {

  lazy val consumer: KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("bootstrap.servers", Context | "kafka.url")
    props.put("group.id", groupId)
    new KafkaConsumer[String, String](props)
  }

  lazy val pool: ExecutorService = Executors.newFixedThreadPool(Integer.parseInt(Context | "thread.size.travel"))
  implicit lazy val executionContext: ExecutionContext = ExecutionContext.fromExecutor(pool)

  override def run(): Unit = {
    consumer.subscribe(util.Collections.singleton(topic))
    logger.info("server start")
    while (Context.working) {
      import java.time.Duration
      val records = consumer.poll(Duration.ofMillis(java.lang.Integer.parseInt(Context | "kafka.timeout")))
      if (records.count != 0) {
        logger.info(s"records count: ${records.count()}")
        records.forEach(record => Future {
//          logger.info("launch a new thread")
//          parse(record.value()) match {
//            case JObject(jo) => logger.info(s"----jo : $jo")
//            case _ => logger.error("err not join")
//          }
          Try(Document(record.value)) match {
            case Failure(parseEx) =>
              logger.error(parseEx)
              trap(record.value)
            case Success(json) =>
              Try(core(parse(json.toJson()))) match {
                case Failure(procEx) =>
                  logger.error(procEx)
                  trap(record.value)
                case Success(_) =>
              }
          }
        })
      }
    }
    logger.info("wait rest tasks ...")
    pool.shutdown()
    logger.info("shutdown ...")
    pool.awaitTermination(8, TimeUnit.SECONDS)
    logger.info("All tasks completed")
    consumer.close()
  }

}

object Server2 {
  var work = false
  def apply(topic: String, group: String, core: JValue => Unit, trap: String => Unit): Server2 = new Server2(topic, group, core, trap)
}
