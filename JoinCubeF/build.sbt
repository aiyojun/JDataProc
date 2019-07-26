name := "JoinCubeF"

organization := "com.jpro"

version := "0.1"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.12.0"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.12.0"

libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"

/// json
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.6"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.6"

/// kafka
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.2.1"

/// mongo
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0"
