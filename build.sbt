import java.util.Properties

val akkaVersion = "2.4.11"
val mesosVersion = "1.0.0"
val curatorVer = "2.11.0"
val playVersion = "2.5.8"
val logbackVersion = "1.1.7"
val zookeeperVersion = "3.4.9"
val commonSettings = Seq(
  scalaVersion := "2.11.8"
)

val appProperties = {
  val prop = new Properties()
  IO.load(prop, new File("project/version.properties"))
  prop
}
lazy val root = (project in file(".")).
  settings(
    commonSettings : _*).
  settings(
    name := "ceph-on-mesos",
    version := appProperties.getProperty("version"),
    resolvers += "Mesosphere Public Repo" at "http://downloads.mesosphere.com/maven",

    libraryDependencies ++= Seq(
      "org.kamranzafar" % "jtar" % "2.3",
      "commons-io" % "commons-io" % "2.5",
      "com.github.kxbmap" %% "configs" % "0.4.3",
      "org.scala-lang.modules" %% "scala-async" % "0.9.5",
      "org.apache.mesos" % "mesos" % mesosVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-experimental" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.play" %% "play-json" % playVersion,
      "com.typesafe.akka" %% "akka-http-experimental" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "org.scaldi" %% "scaldi" % "0.5.7",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "org.rogach" %% "scallop" % "2.0.2",
      "ch.qos.logback" % "logback-classic" % "1.1.7",
      ("org.apache.curator" % "curator-framework" % curatorVer),
      "org.apache.curator" % "curator-recipes" % curatorVer,
      ("org.apache.zookeeper" % "zookeeper" % zookeeperVersion).exclude("org.slf4j", "slf4j-log4j12")
    )
  ).
  dependsOn(marathon)

enablePlugins(JavaAppPackaging)

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
      ("com.twitter" %% "util-zk" % "6.34.0").exclude("org.apache.zookeeper", "zookeeper"),
      "mesosphere" %% "mesos-utils" % "1.1.0-mesos-healthchecks",
      "com.fasterxml.uuid" % "java-uuid-generator" % "3.1.4",
      ("org.apache.zookeeper" % "zookeeper" % zookeeperVersion).exclude("org.slf4j", "slf4j-log4j12")
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

val scalaJSReactVersion = "0.11.2"
val scalaCssVersion = "0.5.0"
val reactJSVersion = "15.3.2"


val uiBuildPath = file("ui") / "js"
lazy val ui = (project in file("ui")).
  enablePlugins(ScalaJSPlugin).
  settings(
    commonSettings: _*).
  settings(
    resolvers += "mmreleases" at "https://artifactory.mediamath.com/artifactory/libs-release-global",
    libraryDependencies ++= Seq("com.github.japgolly.scalajs-react" %%% "core" % scalaJSReactVersion,
      "com.github.japgolly.scalajs-react" %%% "extra" % scalaJSReactVersion,
      "com.github.japgolly.scalacss" %%% "core" % scalaCssVersion,
      "com.github.japgolly.scalacss" %%% "ext-react" % scalaCssVersion,
      "com.github.chandu0101.scalajs-react-components" %%% "core" % "0.5.0",
      "com.mediamath" %%% "scala-json" % "1.0"),


    // React JS itself (Note the filenames, adjust as needed, eg. to remove addons.)
    jsDependencies ++= Seq(

      "org.webjars.bower" % "react-bootstrap" % "0.30.3"
        /        "react-bootstrap.js"
        minified "react-bootstrap.min.js"
        dependsOn "react-with-addons.js"
        commonJSName "ReactBootstrap",

      "org.webjars.bower" % "react" % reactJSVersion
        /        "react-with-addons.js"
        minified "react-with-addons.min.js"
        commonJSName "React",

      "org.webjars.bower" % "react" % reactJSVersion
        /         "react-dom.js"
        minified  "react-dom.min.js"
        dependsOn "react-with-addons.js"
        commonJSName "ReactDOM",

      "org.webjars.bower" % "react" % reactJSVersion
        /         "react-dom-server.js"
        minified  "react-dom-server.min.js"
        dependsOn "react-dom.js"
        commonJSName "ReactDOMServer"),


    // create launcher file ( its search for object extends JSApp , make sure there is only one file)
    persistLauncher := true,

    persistLauncher in Test := false,

    skip in packageJSDependencies := false,

    crossTarget in (Compile, fullOptJS) := uiBuildPath,

    crossTarget in (Compile, fastOptJS) := uiBuildPath,

    crossTarget in (Compile, packageJSDependencies) := uiBuildPath,

    crossTarget in (Compile, packageScalaJSLauncher) := uiBuildPath,

    crossTarget in (Compile, packageMinifiedJSDependencies) := uiBuildPath,

    artifactPath in (Compile, fastOptJS) := ((crossTarget in (Compile, fastOptJS)).value /
      ((moduleName in fastOptJS).value + "-opt.js")),

    scalacOptions += "-feature"
  ).
  dependsOn(
    ProjectRef(uri("git://github.com/timcharper/scalajs-react-bridge.git"), "scalajs-react-bridge"))
