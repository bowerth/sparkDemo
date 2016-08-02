name := """sparkDemo"""

version := "1.0-SNAPSHOT"

// lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  // "org.apache.spark" %% "spark-core" % "2.1.0-SNAPSHOT"
  // "org.apache.spark" %% "spark-core" % "2.1.0"
  // "org.apache.spark" % "spark-core_2.10" % "1.0.0"
  "org.apache.spark" % "spark-core_2.11" % "2.0.0"
  // jdbc,
  // cache,
  // ws,
  // specs2 % Test
  //   ,
  // "org.webjars" %% "webjars-play" % "2.2.0",
  // "org.webjars" % "bootstrap" % "2.3.1"
)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
// resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
// resolvers += "Akka Repository" at "http://repo.akka.io/snapshots/"

// Play provides two styles of routers, one expects its actions to be injected, the
// other, legacy style, accesses its actions statically.
// routesGenerator := InjectedRoutesGenerator

