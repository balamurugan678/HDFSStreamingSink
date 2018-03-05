
name := "HDFSSink"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies ++= {

  Seq(
    "org.apache.spark" % "spark-core_2.10" % "1.6.0",
    "org.apache.spark" % "spark-streaming_2.10" % "1.6.0",
    "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0",
    "org.apache.spark" % "spark-sql_2.10" % "1.6.0",
    "org.json4s" % "json4s-native_2.10" % "3.2.10",
    "org.json4s" % "json4s-jackson_2.10" % "3.2.10",
    "com.google.code.gson" % "gson" % "2.7",
    "org.apache.spark" % "spark-hive_2.10" % "1.6.0",
    "mysql" % "mysql-connector-java" % "5.1.28",
    "com.databricks" %% "spark-avro" % "2.0.1"


  )
}

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)

scalacOptions ++= Seq("-deprecation", "-feature",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps", "-Ywarn-adapted-args",
  "-Ywarn-dead-code", "-Ywarn-inaccessible")

scalacOptions in Test += "-language:reflectiveCalls"

jarName in assembly := "sparky_poc.jar"

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "plugin.xml" => MergeStrategy.rename
  case "overview.html" => MergeStrategy.rename
  case "parquet.thrift" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}