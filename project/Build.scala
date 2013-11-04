import sbt._
import Keys._
import edu.umass.cs.iesl.sbtbase.Dependencies
import edu.umass.cs.iesl.sbtbase.IeslProject._

import play.Project._


object Projects extends Build {

  val vers = "0.1-SNAPSHOT"
  val organization = "org.adamchandra"
  val scalaV = "2.10.2"

  implicit val allDeps: Dependencies = new Dependencies()

  import allDeps._

  def file(s:String) = new java.io.File(s)
  
  // val scalaCommons =    ProjectRef(file("workspace/iesl-scalacommons"), "scalacommons")
  // val boxter =          ProjectRef(file("workspace/boxter"), "boxter")
  
  lazy val slf4jVersion = "latest.release"

  lazy val prjCore = {
    val deps: Seq[ModuleID] =
      Seq(
        ieslScalaCommons("latest.integration"),
        "org.adamchandra" %% "boxter" % "0.1-SNAPSHOT",
        "org.mongodb" %% "casbah" % "2.5.0", 
        // "org.json4s" %% "json4s-native" % "3.0.0",
        "org.scala-lang" % "scala-compiler" % scalaV,
        "org.scala-lang" % "jline" % scalaV,
        "commons-io" % "commons-io" % "2.4",
        "org.scalaz" %% "scalaz-core" % "7.0.3",
        "org.scalaz" %% "scalaz-concurrent" % "7.0.3",
        specs2()
      )

    (Project("scala-console-plus", file("."))
      .ieslSetup(vers, deps, Public, WithSnapshotDependencies, org = organization, conflict = ConflictStrict) 
      .cleanLogging.standardLogging //("!.6.1")
      .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
      .settings(scalacOptions ++= List("-feature", "-language:implicitConversions", "-language:postfixOps", "-language:reflectiveCalls", "-language:existentials"))
      .settings(parallelExecution in Test := false)
      .settings(historyPath <<= baseDirectory(t => Some(t / ".sbt-history")))
      .settings(scalaVersion := scalaV)
    ).dependsOn(
      // scalateCore, scalateCorePlus, scalateUtil, scalaCommons, boxter
    )
  }

}

