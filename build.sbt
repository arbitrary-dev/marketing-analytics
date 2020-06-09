resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

name := "marketing-analytics"

version := "0.0.1"

scalaVersion := "2.12.11"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"
