import sbt.Keys._

ThisBuild / name := "teehr-aggregations"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.rti.teehr"
ThisBuild / exportJars := true

// Dependency versions
val sparkVersion = "4.0.1"
val scalaTestVersion = "3.2.18"

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
      // Spark dependencies (provided - don't bundle Spark itself)
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,

      // Testing dependencies
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test
    ),

    // Assembly settings for creating a fat JAR
    assembly / assemblyOutputPath := target.value / "scala-2.13" / "teehr-aggregations-assembly-0.1.0.jar",
    assembly / assemblyOption ~= { _.withIncludeScala(false) },
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },

    // Compiler settings
    scalacOptions ++= Seq(
      "-feature",
      "-deprecation",
      "-Xfatal-warnings"
    )
  )
