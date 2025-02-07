name := "DataEngineeringAnalysis"
version := "0.1.0"
scalaVersion := "2.11.12"

lazy val sparkVersion = "2.3.0"
lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
)
libraryDependencies ++= commonDependencies
