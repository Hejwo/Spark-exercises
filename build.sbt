name := "Spark-exercises"

version := "0.1"
scalaVersion := "2.11.8"

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
)
