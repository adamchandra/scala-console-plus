package org.adamchandra.sconsplus
package mongodb

import org.adamchandra.sconsplus.core._

import scala.tools.nsc.Settings
import scala.tools.nsc.reporters.ConsoleReporter
import scala.tools.nsc.interpreter.{ ILoop, ReplReporter, Results }
import scala.collection.JavaConversions._
import com.typesafe.scalalogging.slf4j.Logging
import edu.umass.cs.iesl.scalacommons.NonemptyString
import org.joda.time.format.DateTimeFormat
import org.bson.types.BasicBSONList
import com.typesafe.scalalogging.slf4j.Logging
import java.util.UUID
import org.joda.time.DateTime
import ch.qos.logback.classic.LoggerContext
import org.slf4j.LoggerFactory
import ch.qos.logback.core.util.StatusPrinter
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection

import org.adamchandra.boxter.Boxes._

import scalaz.syntax.id._
import scalaz.syntax.monad._
import scalaz.std.function._
import scalaz.syntax.traverse._

import scalaz.{ValidationNel, Traverse, NonEmptyList}
import scalaz.std.list.listInstance

import scalaz.syntax.std.option._
import scalaz.std.option._
import scalaz.std.list._

import scalaz.{Validation, Success, Failure}, Validation._, scalaz.syntax.validation._


object MongoCoreAdmin extends MongoAdminOps {
  class DummyClass()

  def main(args: Array[String]) {

    val iLoop = new ScalaILoop {

      override def printWelcome() {
        echo("""|
                |   OpenReview Console
                |
                |      Type ":examples" for a quick overview, or ":help" for assistance
                |
                |""".stripMargin
        )
      }

      override def prompt = "console> "


      def useDatabase(args: List[String]) {
        if (args.length==0 || args.length>2) {
          echo("specify 1 or 2 arguments")
        } else {
          if (args.length==1)
            setMongoContext(host="localhost", dbname=args(0))
          else if (args.length==2)
            setMongoContext(host=args(0), dbname=args(1))

          // check that this is an existing database
          val allDbs = listDatabases();
          if ((allDbs filter (_.name==mongoContext.dbname)).isEmpty) {
            println("I don't know a db with that name. Existing dbs are:")
            listDatabasesBox()
            // println(allDbs.map(db => ""+db.name+": empty="+db.empty).mkString("\n  ", "\n  ", "\n"))
          } else {
            println("Collections: ")
            println(getCollectionNames().mkString("\n  ", "\n  ", "\n"))
          }
          clearMemo()
          // make sure server-side javascript is up-to-date
          installSystemJavascripts()
          // Setup the model-based access
          initDb(mongoContext.host, mongoContext.dbname)
        }
      }

      import LoopCommand.{nullary, cmd, varargs}

      val additionalCmds = List(
        // :use
        varargs(name="use", usage="<host?> <dbname>", help="set active database", (args:List[String]) =>{
          useDatabase(args);  ()
        }).withLongHelp("""| With one argument, use the specified database on localhost.
                           | With two arguments, specify host and db
                           |""".stripMargin),

        // :dbs
        nullary(name="dbs", help="list databases", () => { listDatabasesBox(); () }),

        // :v/q verbose/quiet
        nullary(name="v", help="verbose output", () => { setVerbose(true); () }),
        nullary(name="q", help="less output (quiet)", () => { setVerbose(false); () }),
        nullary(name="examples", help="a few samples", () => { println(helpExamples) }),

        // :show
        cmd(name="show", usage="<@id>", help="show a record by its @id", (arg:String) => {
          try {
            // val id = uuidfor(arg.toInt)
            // val shown = findById(id)
            list(findById(arg.toInt))
          } catch {
            case e:Exception => "error: "+e.getMessage()
          } 
          ()
        })

      )

      override def commands: List[LoopCommand] =
        standardCommands ++ additionalCmds

      addThunk {
        intp.beQuietDuring {
          intp.addImports(
            "com.mongodb.casbah.Imports._",
            "scalaz.syntax.id._",
            "scalaz.syntax.monad._",
            "scalaz.std.function._",
            "scalaz.std.option._",
            "scalaz.syntax.std.option._"
          )
          // useDatabase(List(""))
        }
      }


      def printTransaction(t:Transaction) {
        t match {
          case TransactionList(desc, ids, f) =>
            println(">>>>  "+desc+"  ========================")
            ids.foreach {id => 
              println(renderUUID(id.asInstanceOf[UUID]))
              println()
            }

          case Transactions(desc, ts@_*) =>
            println(">>>>  "+desc+"  ========================")
            ts.foreach{
              t => printTransaction(t)
            }
        }
      }

      // Try to figure out how to print the last evaluated expression:
      // override def printLastValue(v: Either[Throwable, Object]): Boolean = {

      override def printLastValue(replResult: ReplReturnValue) {
        for {
          result <- replResult.res
        } result match {
          case Right(value) =>
            val handleOutput = value match {
              case t:Traversable[_] => pageStart(t)
              case t:Iterator[_]    => pageStart(t.toTraversable)
              case t:Option[_]      => pageStart(t.toTraversable)
              case o:PagedStreamResult  => pageMore(o); true
              case o:DBObject       => pageStart(Stream(o)); true
              case o:UUID           => pageStart(Stream(o)); true
              case o:Transaction    => printTransaction(o); true
              case o:StringOutput   => println(o.s); true
              case o:Throwable      => println(o.getStackTraceString); true   // why do Throwables end up on the Right instead of the Left??
              case _ => false
            }

            if (!handleOutput) super.printLastValue(replResult)

          case Left(throwable) =>
            super.printLastValue(replResult)
            throwable.printStackTrace(out)
        }
      }
    }


    val settings = ScalaConsole.newSettings[DummyClass]
    
    iLoop.process(settings)
  }
}
