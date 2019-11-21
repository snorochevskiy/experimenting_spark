import sbt._

object Dependencies {

  // Ensure that "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7", is used

  val scalaLangVersion = "2.11"

  val sparkVersion = "2.4.4"

  lazy val apacheHttpClient = "org.apache.httpcomponents" % "httpclient" % "4.5.10"

  lazy val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  // For HOCON configs
  lazy val typeSafeConfig = "com.typesafe" % "config" % "1.3.3"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"


}
