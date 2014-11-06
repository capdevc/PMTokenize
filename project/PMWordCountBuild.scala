import sbt._
import sbt.Keys._

object PMWordCountBuild extends Build {

  lazy val PMWordCount = Project(
    id = "PMWordCount",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "PMWordCount",
      organization := "com.pyaanalytics",
      version := "0.1",
      scalaVersion := "2.10.4",
      // add other settings here
      libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0",
      libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0",
      libraryDependencies += "org.mongodb" % "mongo-java-driver" % "2.11.4",
      retrieveManaged := true,
      resolvers += "Akka Repository" at "http://repo.akka.io/releases/")
  )
}
