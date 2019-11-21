import Dependencies._

scalaVersion := "2.11.12"

resolvers += Resolver.mavenLocal

organization in ThisBuild := "com.github.snorochevskiy"
scalaVersion in ThisBuild := "2.11.12"
version in ThisBuild      := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "dynamic-changes",
    libraryDependencies ++= Seq(
      sparkStreaming % Provided,
      sparkSql % Provided,
      typeSafeConfig,
      scalaTest % Test
    )

  )

mainClass in assembly := Some("com.github.snorochevskiy.playing.spark.Main")
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)

// To merge duplicated artifacts in different artifacts storages: ~/.m2, ~/.cache/coursier, ~/.ivy
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}

