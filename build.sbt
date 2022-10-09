name := "CS441_Homework1"
version := "0.1"
scalaVersion := "3.1.3"

val logbackVersion = "1.4.3"
val sfl4sVersion = "2.0.3"
val typesafeConfigVersion = "1.4.2"
val hadoopVersion = "1.2.1"
val scalacticVersion = "3.2.14"

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-core" % logbackVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.slf4j" % "slf4j-api" % sfl4sVersion,
  "com.typesafe" % "config" % typesafeConfigVersion,
  "org.scalactic" %% "scalactic" % scalacticVersion,
  "org.scalatest" %% "scalatest" % scalacticVersion % Test,
  "org.scalatest" %% "scalatest-featurespec" % scalacticVersion % Test,
  "org.apache.hadoop" % "hadoop-core" %hadoopVersion
)