name := "Evidence-Data-Generator"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.3.1",
  "org.apache.spark" %% "spark-streaming" % "2.0.0",
  "com.typesafe" % "config" % "1.4.0",
  "com.google.code.gson" % "gson" % "2.8.6"
)

assemblyMergeStrategy in assembly := {
  case PathList ("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}