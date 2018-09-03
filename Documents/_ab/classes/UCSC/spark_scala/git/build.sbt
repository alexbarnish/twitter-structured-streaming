name := "twitter-structured-streaming"

version := "1.0"

organization := "org.twitter-structured-streaming"

scalaVersion := "2.11.8"



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-streaming" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-catalyst" % "2.3.0"
)
