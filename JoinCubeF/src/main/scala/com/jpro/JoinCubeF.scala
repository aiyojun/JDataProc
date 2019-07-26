package com.jpro

import org.apache.logging.log4j.scala.Logging
import org.json4s.{JValue, _}
import org.json4s.jackson.JsonMethods._

object JoinCubeF extends Logging {
	def test(args: Array[String]): Unit = {
		println("  ____  _____  ____  _  _  ___  __  __  ____  ____  ____ \n"+
						" (_  _)(  _  )(_  _)( \\( )/ __)(  )(  )(  _ \\( ___)( ___)\n" +
						".-_)(   )(_)(  _)(_  )  (( (__  )(__)(  ) _ < )__)  )__) \n" +
						"\\____) (_____)(____)(_)\\_)\\___)(______)(____/(____)(__)  \n"
		)

		logger.info("JoinCubeF start ...")


//		val s = "{\"name\":\"ming\",\"age\":12}"
//		val json = parse(s)
//
//		def f: JValue => Boolean = x => x \ "hehe" match {
//			case JNothing => false
//			case _ => true
//		}
//		import com.jpro.framework.Wrapper.OptionJValueJValueToWrapper
//		def g: JValue => JValue = x => {
//			x merge JObject("number" -> JInt(12))
//		}
//		val finalJson = Option(json).validate(f).transform(g)
//
//		if (finalJson.nonEmpty) {
//			println(compact(finalJson.get))
//		} else {
//			logger.info("nothing a a a~")
//		}
	}
}
