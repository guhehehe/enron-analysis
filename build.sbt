libraryDependencies ++= Seq(
  "com.sun.mail" % "javax.mail" % "1.5.5",
  "joda-time" % "joda-time" % "2.9.1",
  "org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided",
  "org.easymock" % "easymock" % "3.2" % "test",
  "org.joda" % "joda-convert" % "1.2",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

lazy val root = (project in file(".")).settings(
  name := "enron-analysis",
  version := "1.0",
  scalaVersion := "2.10.5"
)
