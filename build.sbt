val sparkVersion = "3.5.6"
val hadoopVersion= "3.3.4"


ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

ThisBuild / organization := "auth.datalab"
ThisBuild / Test / parallelExecution := false

assembly / test := {}
assembly / mainClass := Some("auth.datalab.siesta.Main")
scalacOptions += "-deprecation"
javacOptions ++= Seq("-source", "17", "-target", "17")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion, //% "provided"
    "org.apache.spark" %% "spark-sql" % sparkVersion 
)
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopVersion //3.0.3
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.520"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.7.0-M11"

lazy val root = (project in file("."))
  .settings(
    name := "SIESTA-CBDeclare",
    // Fork the JVM to avoid SBT background job issues with Hadoop
    fork := true,
    // Enhanced JVM options for Java 17 and Spark 3.5.6
    javaOptions ++= Seq(
      "-Xmx4g",
      "-XX:+UseG1GC",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.ref=ALL-UNNAMED",
      "--add-opens=java.base/java.time=ALL-UNNAMED",
      "--add-opens=java.base/java.time.zone=ALL-UNNAMED",
      "--add-opens=java.management/sun.management=ALL-UNNAMED"
    )
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
