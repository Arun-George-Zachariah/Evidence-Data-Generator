name := "Evidence-Data-Generator"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.3.1",
  "org.apache.spark" %% "spark-streaming" % "1.6.1",
  "com.typesafe" % "config" % "1.4.0"
)

assemblyMergeStrategy in assembly := {
  case PathList ("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}