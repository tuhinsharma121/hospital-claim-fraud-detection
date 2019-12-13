lazy val root = (project in file(".")).settings(
  inThisBuild(List(
    organization := "org.binaize",
    scalaVersion := "2.11.12",
    version := "0.1.0-SNAPSHOT"
  )),
  name := "hospital-claim-fraud-profiler"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.25" % Test
libraryDependencies += "net.liftweb" %% "lift-json" % "3.1.1"
libraryDependencies += "joda-time" % "joda-time" % "2.10.1"
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.8.0"
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.11" % "1.4.0"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"