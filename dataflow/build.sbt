lazy val root = (project in file(".")).
  settings(
    name := "telemetry-example-dataflow",
    version := "0.1",
    scalaVersion := "2.12.4",
    retrieveManaged := true,
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.5.3",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5",
    libraryDependencies += "org.apache.beam" % "beam-runners-direct-java" % "2.3.0",
    libraryDependencies += "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % "2.3.0",
    libraryDependencies += "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % "2.3.0"
  ).
  settings(
    assemblyMergeStrategy in assembly := {
      case "META-INF/io.netty.versions.properties" => MergeStrategy.first
      case PathList("META-INF", "native", xs@_*) => MergeStrategy.first // io.netty
      case PathList("META-INF", "services", xs@_*) => MergeStrategy.filterDistinctLines // IOChannelFactory
      case PathList("META-INF", xs@_*) => MergeStrategy.discard // google's repacks
      case _ => MergeStrategy.first
    }
  )
