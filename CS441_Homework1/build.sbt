ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "CS441_Homework1",
    idePackagePrefix := Some("edu.uic.cs441")
  )

