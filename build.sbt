name := "wikiplag_utils"

version := "1.0"

scalaVersion := "2.10.6"

/*
 * Dependencies
 */

val unbescaped = "org.unbescape" % "unbescape" % "1.1.3.RELEASE"
val commonsCodec = "commons-codec" % "commons-codec" % "1.9"
val mongoDBDriverDep = "org.mongodb" %% "casbah" % "3.1.1"


libraryDependencies ++= Seq(unbescaped, commonsCodec, mongoDBDriverDep)