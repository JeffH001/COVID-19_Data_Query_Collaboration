name := "Project2"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.3"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.3"

scalacOptions += "-deprecation"
