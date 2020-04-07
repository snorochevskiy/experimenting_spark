// Plugin that fetches correct spark version and defines artifact name
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
//addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")

// Plugin for building "fat" jar
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

// For "sbt coursierDependencyTree" command
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.3")

libraryDependencies += "org.scala-sbt" %% "scripted-plugin" % sbtVersion.value
