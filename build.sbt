name := "spark-test"
version := "1.0"
scalaVersion := "2.12.12"
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.2.2",
  "org.apache.spark" % "spark-sql_2.12" % "3.2.2",
  "org.apache.spark" % "spark-streaming_2.12" % "3.2.2",
  "org.apache.spark" % "spark-mllib_2.12" % "3.2.2",
  "org.jmockit" % "jmockit" % "1.49" % "test",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "org.mockito" % "mockito-core" % "2.28.2" % "test"
)