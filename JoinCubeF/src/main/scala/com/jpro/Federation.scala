package com.jpro

import com.jpro.framework.Server
import com.jpro.resource.MagicBox
import org.apache.logging.log4j.scala.Logging
import sun.misc.Signal

object Federation extends Logging {
  def main(args: Array[String]): Unit = {
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
    Server().prepare().start()

    MagicBox.recycleResource()
  }
}
