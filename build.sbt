val akkaVersion = "2.4.9"
val mesosVersion = "1.0.0"
val curatorVer = "2.11.0"
val playVersion = "2.5.8"
val logbackVersion = "1.1.7"

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
      "com.typesafe.play" %% "play-json" % playVersion,
      "com.typesafe.akka" %% "akka-http-experimental" % akkaVersion,
      "org.scaldi" %% "scaldi" % "0.5.7",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "org.rogach" %% "scallop" % "2.0.2",
      "ch.qos.logback" % "logback-classic" % "1.1.7",
      "org.apache.curator" % "curator-framework" % curatorVer
    )
  ).
  dependsOn(marathon)

lazy val marathon = (project in file("marathon-submodule/")).
  settings(
    commonSettings: _*).
  settings(
    resolvers ++= Seq(
      "Mesosphere Public Repo" at "http://downloads.mesosphere.com/maven"
    ),
    libraryDependencies ++= Seq(
      "org.apache.mesos" % "mesos" % mesosVersion,
      "com.google.protobuf" % "protobuf-java" % "2.6.1",
      "com.wix" %% "accord-core" % "0.5",
      "com.typesafe.play" %% "play-json" % playVersion,
      "ch.qos.logback" % "logback-core" % logbackVersion,
      "org.apache.curator" % "curator-framework" % curatorVer,
      "com.twitter" %% "util-zk" % "6.34.0",
      "mesosphere" %% "mesos-utils" % "1.1.0-mesos-healthchecks",
      "com.fasterxml.uuid" % "java-uuid-generator" % "3.1.4"
    ),
    unmanagedSources in Compile ++= Seq(
      baseDirectory.value / "marathon/plugin-interface/src/main/scala/mesosphere/marathon/plugin/PathId.scala",
      baseDirectory.value / "marathon/src/main/java/mesosphere/marathon/Protos.java",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/Features.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/api/v2/Validation.scala", // move to util
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/core/task/Task.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/core/task/TaskStateOp.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/core/task/state/MarathonTaskStatus.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/core/task/state/MarathonTaskStatusMapping.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/state/FetchUri.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/state/MarathonState.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/state/PathId.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/state/ResourceRole.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/state/Timestamp.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/marathon/state/Volume.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/util/Logging.scala",
      baseDirectory.value / "marathon/src/main/scala/mesosphere/util/state/FrameworkId.scala"
    ),
    unmanagedSourceDirectories in Compile ++= Seq(
      baseDirectory.value / "marathon/src/main/scala/mesosphere/mesos/"
    )
  )
