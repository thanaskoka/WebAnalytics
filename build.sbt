name := "Web Analytics"
version := "1.0"
scalaVersion := "2.10.4"

assemblyJarName in assembly := "WebAnalytics.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.2" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.2" % "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.2" % "provided",
  "com.databricks" % "spark-csv_2.10" % "1.5.0",
  "com.typesafe" % "config" % "1.3.1",
  "com.microsoft.azure" % "azure-storage" % "4.4.0",
  "com.databricks" % "spark-xml_2.10" % "0.3.5",
  "org.json4s"%"json4s-native_2.10"%"3.2.10"
)

// Copy all managed dependencies to <build-root>/lib_managed/
retrieveManaged := true
//