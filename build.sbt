name := "offset-management"

version := "0.1"

scalaVersion := "2.10.5"

val sparkVersion = "1.6.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"

// HiveContext support for window aggregation
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion

// csv support
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"

// zookeeper support
libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.6"

// Assembly Setting
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}