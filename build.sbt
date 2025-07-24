val sparkVersion = "3.5.1"
val hadoopVersion= "3.3.4"


ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

ThisBuild / organization := "auth.datalab"
ThisBuild / Test / parallelExecution := false

assembly / test := {}
assembly / mainClass := Some("auth.datalab.siesta.Main")
scalacOptions += "-deprecation"
javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion, //% "provided"
    "org.apache.spark" %% "spark-sql" % sparkVersion )
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopVersion //3.0.3
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "DeclareMiningIncrementally"
  )

assembly / assemblyMergeStrategy:= {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case manifest if manifest.contains("MANIFEST.MF") =>
    MergeStrategy.discard
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
    MergeStrategy.concat
  case deckfour if deckfour.contains("deckfour") || deckfour.contains(".cache") =>
    MergeStrategy.last
  case x =>
    MergeStrategy.last
}
