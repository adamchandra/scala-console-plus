package net.openreview.model.raw.casbah.admin

import scala.Predef._
import scala.{Double, Some}
import edu.umass.cs.iesl.scalacommons.NonemptyString
import org.joda.time.format.DateTimeFormat
import org.bson.types.BasicBSONList
import com.typesafe.scalalogging.slf4j.Logging
import java.util.UUID
import net.openreview.model.raw.Storage._
import net.openreview.model.raw._
import net.openreview.model.raw.casbah._
import org.joda.time.DateTime
import ch.qos.logback.classic.LoggerContext
import org.slf4j.LoggerFactory
import ch.qos.logback.core.util.StatusPrinter
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection

import net.openreview.util.boxes.Boxes._

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
import net.openreview.model.admin.{ScalaConsole, ReplReturnValue, ScalaILoop, IntegrityTest}
import net.openreview.model.users.User
import net.openreview.OpenreviewCoreServices

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait MongoAdminOps extends Queries with Formatting with Logging {


  def initDb(host: String, dbname: String) {
    val lc:LoggerContext  =LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]
    // StatusPrinter.print(lc)
    
    // logger.info("[openreview] using MongoDB backend storage: %s".format(this.getClass))
    println("[openreview] using MongoDB backend storage: %s".format(this.getClass))

    com.mongodb.casbah.commons.conversions.scala.RegisterConversionHelpers()
    com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers()
    net.openreview.model.mongodb.RegisterURLHelpers()

    StorageSetter(new CasbahStorage(MongoConnection(host), dbname))
  }


  def copyDatabase(fromHost: String, fromDb: String, toDb: String) {
    val result = conn("admin").command(Map(
      "copydb" -> 1,
      "fromhost" -> fromHost,
      "fromdb" -> fromDb,
      "todb" -> toDb
    ))

    val rdbo = new MongoDBObject(result)
    println(rdbo)
  }

  def listDatabasesBox() {

    def dblistBox(implicit dbo: MongoDBObject) = {
      keyValBox[String]("serverUsed") % indent()(
        vjoin()(
          getTraversable[DBObject]("databases").toList map (
            o => dbBox(new MongoDBObject(o))
          ):_*
        )
      )
    }

    def dbBox(implicit dbo: MongoDBObject) = vjoin()(
      valBox[String]("name"), indent()( vjoin()(
        keyValBox[Int]("sizeOnDisk"),
        keyValBox[Boolean]("empty")
      ))
    )

    val result = conn("admin").command(
      Map("listDatabases" -> 1 )
    )
    val rdbo = new MongoDBObject(result)

    dblistBox(rdbo)  |> render |> println
  }

  case class MongoDBInfo(name: String, sizeOnDisk: Double, empty: Boolean)

  def listDatabases(): List[MongoDBInfo] = {
    val result = conn("admin").command(
      Map("listDatabases" -> 1)
    );
    (getTraversable[DBObject]("databases")(result)).toList.map{dbo =>
      MongoDBInfo(
        dbo.getAs[String]("name").get,
        dbo.getAs[Double]("sizeOnDisk").get,
        dbo.getAs[Boolean]("empty").get
      )
    }
  }


  sealed trait ReplResult {
    def header: Option[String] = None
    def footer: Option[String] = None 
    def renderer: Any => String = _.toString
  }

  type RenderFunction = Any => String

  case object EmptyResult extends ReplResult

  case class PagedStreamResult(
    curr: Stream[_], 
    rest: Stream[_], 
    currOffset: Int,
    override val renderer: RenderFunction, 
    ctype: String, 
    vtype: String
  ) extends ReplResult {
    override def header = {
      Some("Displaying results "+(currOffset+1)+"-"+(currOffset+curr.length)+" of type "+vtype)
    }
    override def footer = {
      if (rest.isEmpty) Some("No more results")
      else Some("Type `more` for more results")
    }
  }

  var pagerSize = 8

  var _replResult: ReplResult = EmptyResult

  def more: ReplResult = {
    _replResult = _replResult match {
      case EmptyResult => EmptyResult
      case PagedStreamResult(curr, rest, offset, r, tc, vc) =>
        PagedStreamResult(rest.take(pagerSize), rest.drop(pagerSize), offset+pagerSize, r, tc, vc)
    }
    _replResult
  }

  def renderUUID(uuid:UUID):String = {
    render(hjoin(center1, sep="  <-(and)->   ")((for {
      ob <- findById(uuid).toList
    } yield mongoDboBox(ob, Summary)):_*))
  }

  // Called whenever repl is trying output a value of type Traversable[_]
  def pageStart(t:Traversable[_]): Boolean = {
    val tclass = t.getClass.getName()
    val tvclass = t.headOption.map(_.getClass().getName).getOrElse("<empty>")

    val (renderer:RenderFunction, handle:Boolean) = tvclass match {
      case "com.mongodb.BasicDBObject" => 
        ((v:Any) => renderDbo(new MongoDBObject(v.asInstanceOf[DBObject]), Summary)) -> true

      case "java.util.UUID" => { v:Any => 
        renderUUID(v.asInstanceOf[UUID])
      } -> true

      case _ =>
        ((v:Any) => render(tbox("<not handled>"))) -> false
    }

    if (handle) {
      _replResult = PagedStreamResult(t.toStream.take(pagerSize), t.toStream.drop(pagerSize), 0, renderer, tclass, tvclass)
      pageMore(_replResult)
    }

    handle
  }


  def pageMore(rr:ReplResult) {
    rr match {
      case EmptyResult => EmptyResult
      case m@PagedStreamResult(curr, rest, offset, r, tc, vc) => // stream, r) =>
        m.header.foreach(println(_))
        curr foreach {x => println(r(x)); println()}
        m.footer.foreach(println(_))
        println()
    }
  }

}
