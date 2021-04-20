name := "spark-excel-reader"
version := "2.0.0"
organization := "com.xorbit"
scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.scalatest" %% "scalatest" % "3.1.1" % Test,
  "org.apache.poi" % "poi" % "4.1.0" % Compile,
  "org.apache.poi" % "poi-ooxml" % "4.1.0"  % Compile,
  "com.monitorjbl" % "xlsx-streamer" % "2.1.0" % Compile,
  "org.apache.commons" % "commons-compress" % "1.19" % Compile,
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.8" % Compile
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.poi.**" -> "shadexorbit.poi.@1").inAll,
  ShadeRule.rename("com.monitorjbl.**" -> "shadexorbit.monitorjbl.@1").inAll,
  ShadeRule.rename("com.fasterxml.jackson.**" -> "shadexorbit.jackson.@1").inAll,
  ShadeRule.rename("org.apache.commons.compress.**" -> "shadexorbit.commons.compress.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("com", "xorbit", xs @ _*) => MergeStrategy.last
  case PathList("shadexorbit", "poi", xs @ _*) => MergeStrategy.last
  case PathList("shadexorbit", "monitorjbl", xs @ _*) => MergeStrategy.last
  case PathList("shadexorbit", "jackson", xs @ _*) => MergeStrategy.last
  case PathList("shadexorbit", "commons", "compress", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", "xmlbeans", xs @ _*) => MergeStrategy.last
  case PathList("org", "openxmlformats", "schemas", xs @ _*) => MergeStrategy.last
  case PathList("schemaorg_apache_xmlbeans", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last  // Added this for 2.1.0 I think
  case x => MergeStrategy.discard
}

// Publish to GitHub
//githubOwner := "kpmatta"
//githubRepository := "spark-excel"
//githubTokenSource := TokenSource.GitConfig("github.token")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

test in assembly := {}