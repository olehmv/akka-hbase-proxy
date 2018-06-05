name := "akka-hbase-proxy"

version := "0.1"

scalaVersion := "2.11.11"

val akkaVersion = "2.5.12"

val akkaHttpVersion = "10.0.13"



libraryDependencies ++= Seq(

  "eu.bitwalker" % "UserAgentUtils" % "1.14",

  "com.typesafe" % "config" % "1.3.2",

  "com.typesafe.akka" %% "akka-stream" % akkaVersion,

  "com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion,

  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,

  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,

  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion,

  "org.apache.hbase" % "hbase-server" % "1.2.6" ,

  "org.apache.hbase" % "hbase-client" % "1.2.6",

  "org.apache.hbase" % "hbase-common" % "1.2.6",

  "org.apache.hadoop" % "hadoop-common" % "2.7.3"

)