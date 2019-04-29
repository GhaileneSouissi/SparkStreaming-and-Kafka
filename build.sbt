name := "Spark-Streaming"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"

fork in run := true
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "1.1.1",
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",
  "org.apache.kafka" %% "kafka" % "2.1.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.11.2",
  "com.typesafe" % "config" % "1.3.4"


)