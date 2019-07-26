package com.jpro.framework

import org.apache.logging.log4j.scala.Logging

private class Server extends Logging {

  def prepare(): Server = {

    this
  }

  def start(): Unit = {
    logger.info("=================================");
    logger.info("core service start");
  }

}

object Server {
  def apply(): Server = new Server()
}