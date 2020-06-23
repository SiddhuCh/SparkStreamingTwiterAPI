name := "TwiterSparkStreaming"

version := "0.1"

scalaVersion := "2.11.12"
//Spark dependencies
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.1"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.2"

//Twitter dependencies
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.1"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.774"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.1.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.1.1"


dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"