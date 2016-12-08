name := "wikiplag_utils"

version := "1.0"
scalaVersion := "2.10.4"

/*
 * Dependencies
 * libraryDependencies += "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.3.2"
 */
val testDependencies = Seq(
  "org.slf4j" % "slf4j-simple" % "1.7.21" % "test",
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)
val unbescaped = "org.unbescape" % "unbescape" % "1.1.3.RELEASE"
val commonsCodec = "commons-codec" % "commons-codec" % "1.9"
val mongoDBDriverDep = "org.mongodb" %% "casbah" % "3.1.1" // + core, query and commons ?!

val sparkCoreDep = "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
val sparkSQLDep = "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"

val stratioSparkDep = "com.stratio.datasource" %% "spark-mongodb" % "0.12.0" exclude("org.scala-lang", "scala-compiler")


libraryDependencies ++= testDependencies
libraryDependencies ++= Seq(
  unbescaped,
  commonsCodec,
  mongoDBDriverDep,
  sparkCoreDep,
  sparkSQLDep,
  stratioSparkDep
)
