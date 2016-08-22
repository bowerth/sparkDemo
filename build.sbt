name := """sparkDemo"""

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"
// scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  // "org.apache.spark" %% "spark-core" % "2.1.0-SNAPSHOT"
  // "org.apache.spark" %% "spark-core" % "2.1.0"
  // "org.apache.spark" % "spark-core_2.10" % "1.0.0"
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
  // "org.apache.hadoop" % "hadoop-aws" % "2.6.0",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.2",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.10
  "org.apache.spark" % "spark-sql_2.10" % "2.0.0"

)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

