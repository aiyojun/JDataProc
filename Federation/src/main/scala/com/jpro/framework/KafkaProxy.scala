package com.jpro.framework

import java.util
import java.util.Properties
import java.util.concurrent.ArrayBlockingQueue

import com.jpro.resource.Context
import com.jpro.util.BaseBlock
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.logging.log4j.scala.Logging

class KafkaProxy(q: ArrayBlockingQueue[BaseBlock]) extends Thread with Logging {
  val sharedQueue: ArrayBlockingQueue[BaseBlock] = q

  var consumer: KafkaConsumer[String, String] = _

  def prepare(topic: String, groupId: String): KafkaProxy = {
    val props = new Properties()
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("bootstrap.servers", Context | "kafka.url")
    props.put("group.id", groupId)
    consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singleton(topic))
    this
  }

  override def run(): Unit = {
    logger.info("\033[33;1mKafkaProxy::run\033[0m")
    while (Context.working) {
      import java.time.Duration
      val records = consumer.poll(Duration.ofMillis(java.lang.Integer.parseInt(Context | "kafka.link.timeout")))
      if (records.count != 0) {
        records.forEach(record => {
          try {
            sharedQueue.put(BaseBlock(record.key, record.value))
          }
          catch {
            case e: InterruptedException =>
              logger.error("Push BaseBlock - " + e)
          }})
      }
    }
    consumer.close()
    logger.info("\033[33;1mKafkaProxy::run exiting...\033[0m")
  }

  def waitThread(): Unit = {
    this.join()
  }
}
