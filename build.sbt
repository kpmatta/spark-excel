name := "spark-excel-reader"

version := "1.0.0-SNAPSHOT"

organization := "com.xorbit"

scalaVersion := "2.12.10"

val sparkVersion = "2.4.4"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.1.1" % Test,
  "org.apache.poi" % "poi" % "4.1.0" % Compile,
  "org.apache.poi" % "poi-ooxml" % "4.1.0"  % Compile,
  "com.monitorjbl" % "xlsx-streamer" % "2.1.0" % Compile,
  "org.apache.commons" % "commons-compress" % "1.19" % Compile,
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.8" % Compile
)

//assemblyShadeRules in assembly := Seq(
//  ShadeRule.rename("org.apache.poi.**" -> "shadexorbit.poi.@1").inAll,
//  ShadeRule.rename("com.monitorjbl.**" -> "shadexorbit.monitorjbl.@1").inAll
//)