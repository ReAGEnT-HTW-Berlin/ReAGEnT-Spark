name := "ReAGEnT-Spark"

version := "0.1"

scalaVersion := "2.12.13"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++=Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1"
)
