name := """sparkDemo"""

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"
// scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  // "org.apache.spark" %% "spark-core" % "2.1.0-SNAPSHOT"
  // "org.apache.spark" %% "spark-core" % "2.1.0"
  // "org.apache.spark" % "spark-core_2.10" % "1.0.0"
  "com.google.code.findbugs" % "jsr305" % "3.0.0",
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
  "org.apache.spark" % "spark-hive_2.11" % "2.0.0",
  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
  // "org.apache.hadoop" % "hadoop-aws" % "2.6.0",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.2",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.6"
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.10
  // "org.apache.spark" % "spark-sql_2.10" % "2.0.0"
)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
// resolvers += "maven2" at "http://central.maven.org/maven2/"

excludeFilter in Compile in unmanagedSources := ".#*" || "FromXml.scala"
