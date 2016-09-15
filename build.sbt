val akkaVersion = "2.4.9"
val mesosVersion = "1.0.0"

val commonSettings = Seq(
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(
    commonSettings : _*).
  settings(
    name := "ceph-on-mesos",

    resolvers += "Mesosphere Public Repo" at "http://downloads.mesosphere.com/maven",

    libraryDependencies ++= Seq(
      "org.apache.mesos" % "mesos" % mesosVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.play" %% "play-json" % "2.5.7",
      "com.typesafe.akka" %% "akka-http-experimental" % akkaVersion,
      "org.scaldi" %% "scaldi" % "0.5.7",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "org.rogach" %% "scallop" % "2.0.2",
      "org.apache.curator" % "curator-framework" % "3.2.0"))
