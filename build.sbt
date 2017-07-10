name := "DataCampXXXS"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark"           %% "spark-core"            % sparkVersion   ,
  "org.apache.spark"           %% "spark-mllib-local"     % sparkVersion   ,
  "org.apache.spark"           %% "spark-mllib"           % sparkVersion   ,
  "org.apache.spark"           %% "spark-sql"             % sparkVersion   ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion ,
  "org.apache.spark" %% "spark-hive" % sparkVersion ,
  "org.scalatest"              %% "scalatest"             % "3.0.0",
  "org.scalaz"                 %% "scalaz-core"           % "7.2.6",
  "org.apache.commons"          % "commons-lang3"         % "3.4",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j"   % "2.1.2"
)

//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
//
//test in assembly := {}
//
//parallelExecution in Test := false