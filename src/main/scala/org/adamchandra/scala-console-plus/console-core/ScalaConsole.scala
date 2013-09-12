package net.openreview.model.admin

import scala.tools.nsc.Settings
import scala.tools.nsc.reporters.ConsoleReporter
import scala.tools.nsc.interpreter.{ ILoop, ReplReporter, Results }
import scala.collection.JavaConversions._
import com.typesafe.scalalogging.slf4j.Logging

object ScalaConsole {
  def newSettings[DummyClassInDesiredClassloaderPath : Manifest]:Settings = {
    val settings = new Settings
    settings.deprecation.value = true
    settings.embeddedDefaults[DummyClassInDesiredClassloaderPath]
    settings.lint.value = true

    lazy val urls = java.lang.Thread.currentThread.getContextClassLoader match {
      case cl: java.net.URLClassLoader => cl.getURLs.toList
      case _ => sys.error("classloader is not a URLClassLoader")
    }
    lazy val classpath = urls map {_.toString}
    settings.classpath.value = classpath.distinct.mkString(java.io.File.pathSeparator)
    settings
  }

  val results = Results

}

case class ReplReturnValue(
  print: Option[Either[Throwable, AnyRef]], 
  res: Option[Either[Throwable, AnyRef]]
)

class ScalaILoop extends ILoop with Logging {

  override def prompt = "scala> "

  override def printWelcome() {
    echo("Welcome to the Scala Console")
  }

  var scalaIntp: ScalaInterpreter = _

  override def createInterpreter() {
    if (addedClasspath != "")
      settings.classpath.append(addedClasspath)
    scalaIntp = new ScalaInterpreter
    intp = scalaIntp
  }

  import scala.collection.mutable.ListBuffer

  var verbose = false

  var lastReplMessage = ListBuffer[String]()

  /**Overriden to print out the value evaluated from the specified line. */
  override def command(line: String): Result = {
    lastReplMessage.clear()
    val result = super.command(line)

    if (result.keepRunning && result.lineToRecord.isDefined) {
      printLastValue(scalaIntp.lastValue)
    }
    result
  }


  def setVerbose(b:Boolean) = verbose = b

  // def printLastValue(replResult: Either[Throwable, Object]): Boolean = {
  def printLastValue(replResult: ReplReturnValue) {
    // println("lr= \n" + lastReplMessage.mkString("\n") )
    // println("\n")

    replResult.print match {
      case Some(x) => x match {
        case Right(y) => println(y)
        case Left(y) => {
          println(y.getStackTraceString)
          logger.error("Error", y)
        }
      }
      case None => replResult.res match {
        case Some(x) => x match {
          case Right(y) => println(y)
          case Left(y) => {
            println(y.getStackTraceString)
            logger.error("Error", y)
          }
        }
      }
    }
  }

  class ScalaInterpreter extends ILoopInterpreter {

    override lazy val reporter: ReplReporter = new ReplReporter(this) {
      override def printMessage(msg: String) {
        lastReplMessage += msg
        if (verbose || (msg != null && msg.startsWith("<console>"))) {
          super.printMessage(msg)
        }
      }
    }
    def prevRequest: Option[Request] = prevRequestList.lastOption
    
    def methodExists(m:String, r:Request): Boolean = {
      !r.lineRep.evalClass.getMethods.filter(_.getName==m).isEmpty
    }


    /**Returns the last value evaluated by this interpreter. See https://issues.scala-lang.org/browse/SI-4899 for details. */
    // def lastValue: Either[Throwable, AnyRef] = {
    def lastValue: ReplReturnValue = {
      val pr = prevRequest.get
      ReplReturnValue(
        if (methodExists("$print", pr)) Some(pr.lineRep.callEither("$print")) else None,
        if (methodExists("$result", pr)) Some(pr.lineRep.callEither("$result")) else None
      )
    }

  }

}




