import sbt._
import sbt.Keys._

object PMWordCountBuild extends Build {

  lazy val PMTokenizeBuild = Project(
    id = "PMTokenize",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "PMTokenize",
      organization := "com.pyaanalytics",
      version := "0.1",
      scalaVersion := "2.10.4",
      // add other settings here
      libraryDependencies ++=  Seq(
        "com.propensive" %% "rapture-json-jackson" % "1.0.6",
        "org.apache.spark" %% "spark-core" % "1.1.0",
        "org.apache.hadoop" % "hadoop-client" % "2.2.0",
        "org.scalanlp" %% "epic" % "0.2",
        "org.mongodb" % "mongo-java-driver" % "2.11.4"
      ),
      retrieveManaged := true,
      resolvers ++= Seq(
        "ScalaNLP Maven2" at "http://repo.scalanlp.org/repo",
        "Akka Repository" at "http://repo.akka.io/releases/"
      )
    )
  )
}
