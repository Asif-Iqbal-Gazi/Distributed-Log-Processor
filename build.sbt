name := "CS441_Homework1"
version := "0.1"
scalaVersion := "3.1.3"

val typesafeConfigVersion = "1.4.2"
val hadoopVersion = "1.2.1"

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}



libraryDependencies ++= Seq(
  "com.typesafe" % "config" % typesafeConfigVersion,
  "org.apache.hadoop" % "hadoop-core" %hadoopVersion
)