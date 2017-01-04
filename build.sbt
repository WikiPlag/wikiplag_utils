name := "wikiplag_utils"

version := "1.0"
scalaVersion := "2.10.4"

/*
 * Dependencies
 */
val testDependencies = Seq(
  "org.slf4j" % "slf4j-simple" % "1.7.21" % "test",
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)
val unbescaped = "org.unbescape" % "unbescape" % "1.1.3.RELEASE"
val commonsCodec = "commons-codec" % "commons-codec" % "1.9"
val mongoDBDriverDep = "org.mongodb" %% "casbah" % "3.1.1" // + core, query and commons ?!

val sparkCoreDep = "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"
val sparkSQLDep = "org.apache.spark" %% "spark-sql" % "1.5.0" % "provided"

val mongoDBHadoopCore = ("org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.5.1")
  .exclude("commons-logging", "commons-logging")
  .exclude("commons-beanutils", "commons-beanutils-core")
  .exclude("commons-collections", "commons-collections")


libraryDependencies ++= testDependencies
libraryDependencies ++= Seq(
  unbescaped,
  commonsCodec,
  mongoDBDriverDep,
  sparkCoreDep,
  sparkSQLDep,
  mongoDBHadoopCore
)
