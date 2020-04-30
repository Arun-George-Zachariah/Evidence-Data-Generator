name := "Evidence-Data-Generator"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.2.0",
)

assemblyMergeStrategy in assembly := {
  case PathList ("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}