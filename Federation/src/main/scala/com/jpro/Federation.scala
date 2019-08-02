package com.jpro

import com.jpro.framework.{Server, Server2}
import com.jpro.processor.APro
import com.jpro.resource.{Context, MagicBox}
import org.apache.logging.log4j.scala.Logging
import sun.misc.Signal

object Federation extends Logging {
  def main(args: Array[String]): Unit = {
//    Signal.handle(new Signal("INT"), _ => {
//      logger.info("program received [ INT ] signal")
//      Server2.work = false
//    })
//    val server2 = Server2("travel", "travel-group", APro.process, APro.trap)
//    server2.run()
//    logger.info("----started")
//    Thread.sleep(10000)
//    System.exit(2)

    println(
      " ____  ____  ____  ____  ____    __   ____  ____  _____  _  _ \n" +
      "( ___)( ___)(  _ \\( ___)(  _ \\  /__\\ (_  _)(_  _)(  _  )( \\( )\n" +
      " )__)  )__)  )(_) ))__)  )   / /(__)\\  )(   _)(_  )(_)(  )  ( \n" +
      "(__)  (____)(____/(____)(_)\\_)(__)(__)(__) (____)(_____)(_)\\_)\n"
    )
    logger.info("Federation start ...")

    MagicBox.initializeResource(args)

    Signal.handle(new Signal("INT"), _ => {
      logger.info("program received [ INT ] signal")
      MagicBox.recycleResource()
    })

    /// TODO: launch main program instance
    Context.working = true
//    val server = Server()
//    server.prepare().start()
    val server = Server2("travel", "travel-group", APro.process, APro.trap)
    server.run()

//    MagicBox.recycleResource()
//    server.waitThread()
    logger.info("\033[36;1mrecycle complete, program exit.\033[0m")
  }
}
