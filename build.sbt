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
val mongoDBDriverDep = "org.mongodb" %% "casbah" % "3.1.1"

libraryDependencies ++= testDependencies
libraryDependencies ++= Seq(
  unbescaped,
  commonsCodec,
  mongoDBDriverDep)
