package net.openreview.model.raw.casbah.admin

import com.mongodb.casbah.Imports._
import scala.Some
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


// record formatting varieties:
sealed trait Format

// iconic1 : 1 line representation
case object FullDescription extends Format
// Textual description of object
case object ShortDescription extends Format
// summary : standard multi-line representation when listing
case object Summary extends Format


/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait Formatting extends Queries {

  def keyValBox[A](f:String)(implicit dbo: MongoDBObject, m:Manifest[A]): Box = {
    (dbo.getAs[A](f) map (
      (v:A) => tbox(f + ": "+v.toString))
    ) |> boxOrEmpty
  }

  def valBox[A](f:String)(implicit dbo: MongoDBObject, m:Manifest[A]): Box = {
    (for {
      v <- dbo.getAs[A](f)
    } yield {
      tbox(v.toString)
    }) getOrElse(
      tbox("<none>")
    )
  }



  def boxOrMessage(msg:String): Option[Box] => Box = ob => ob.getOrElse(tbox(msg))
  def boxOrEmpty: Option[Box] => Box = ob => ob.getOrElse(nullBox)

  def asDBList(f:String)(implicit dbo: MongoDBObject) = dbo.getAs[MongoDBList](f)

  def memoizedIdBox(implicit dbo: MongoDBObject, format: Format): Box = {
    val id = getUUID("_id")
    val memoId = memoUUID(id)
    tbox("[") + tbox(memoId + " @ " + id) + tbox("]")
  }

  def getEventProcessorBox(s:String)(implicit dbo: MongoDBObject): Option[Box] =
    getAsUUID(s) >>= find(eventprocessors) >>= (o => Some(eventProcessorBox(o, FullDescription)))
  
  def getEventBox(s:String)(implicit dbo: MongoDBObject): Option[Box] =  
    getAsUUID(s) >>= find(events) >>= (o => Some(recordBox(o, FullDescription)))

  // TODO: this can be made more generic than hard-coding the field names
  // TODO: if type=anonymizer, also print the subscriber (the one made anonymous)
  def iconic1Field(field:String)(implicit dbo: MongoDBObject): Box = {
    field match {
      case "creator"
         | "eventProcessor"
         | "authors"
         | "subscriber"
         | "cc"
         | "target" => getEventProcessorBox(field) |> boxOrEmpty

      case "doc" => getDocumentBox(field) |> boxOrEmpty

      case "wrapped"  => getEventBox(field) |> boxOrEmpty

      case "_history"  => tbox("_history: ...")

      case _ => (for {
        v <- dbo.get(field)
      } yield {
        if (v.isInstanceOf[UUID]) {
          recordBox(v.asInstanceOf[UUID].findOne, FullDescription)
        } else if (v.isInstanceOf[DateTime]) {
          val fmt = DateTimeFormat.forPattern("EEE MMM d yyyy hh:mm:ss aa")
          tbox(fmt.print(v.asInstanceOf[DateTime]))
        } else if (v.isInstanceOf[String]) {
          tbox(v.toString.oneLine.clip())
        } else {
          tbox(v.toString.oneLine.clip())
        }
      }) |> boxOrEmpty
    }
  }


  def recordBox(implicit dbo: MongoDBObject, format: Format): Box = {
    val keys = dbo.keySet -- Set("_id", "type", "_history" ) 
    val uuids = for { 
      k <- keys.toList
      v <- dbo.get(k) if v.isInstanceOf[UUID] 
    } yield k

    val lists = for { 
      k <- (keys -- uuids).toList 
      v <- dbo.get(k) if v.isInstanceOf[BasicBSONList] 
    } yield k

    val dbos = for { 
      k <- (keys -- uuids -- lists).toList 
      v <- dbo.get(k) if v.isInstanceOf[BasicDBObject] || v.isInstanceOf[DBObject] 
    } yield k

    val other = (keys -- Set((uuids ++ lists ++ dbos):_*)).toList

    format match {
      case FullDescription =>
        val sdf = getAsUUID("_id")

        getAsUUID("_id").fold(
          valBox[String]("type"))(
          _ => tbox(modelDescription) +| valBox[String]("type") +| memoizedIdBox)

      case ShortDescription =>
        getAsUUID("_id").cata(
          some= _ => tbox(modelDescription.clip(20)) +| valBox[String]("type") +| memoizedIdBox,
          none= valBox[String]("type")
        )
      case Summary  =>
        val boxes1 = for {
          k <- uuids
        } yield tbox(k.toString) -> iconic1Field(k)

        val boxes2 = for {
          k <- other
          v <- dbo.get(k)
        } yield tbox(k.toString) -> iconic1Field(k)

        val boxes3 = for {
          k <- dbos
          v <- dbo.get(k)
        } yield {
          recordBox(v.asInstanceOf[DBObject], Summary)
        }

        val boxesLists = for {
          k <- lists
          v <- dbo.get(k)
        } yield tbox(k.toString) -> tbox("size="+v.asInstanceOf[BasicBSONList].size)

        val boxes = boxes1 ++ boxes2 ++ boxesLists


        vjoin()(
          recordBox(dbo, FullDescription),
          indent()(
            vjoin()(
              hjoin()(
                borderLeftRight("", " : ")(
                  vjoin()(boxes.map(_._1):_*)
                ),
                vjoin()(boxes.map(_._2):_*)
              ),
              vjoin()(boxes3:_*)
            )
          )
        )
    }
  }

  //def eventBox(implicit dbo: MongoDBObject, format: Format): Box = {
  //  format match {
  //    case Iconic1 =>
  //      recordBox(dbo, Description) +| recordBox
  //    case Description =>
  //      modelDescription
  //    case Summary  =>
  //      recordBox(dbo, format)
  //  }
  //}


  def linkedAccountBox(implicit dbo: MongoDBObject, format: Format): Box = {
    recordBox
  }

  def emailSummary(implicit dbo: MongoDBObject): Box = {
    valBox("email")
  }

  type BoxerFn = MongoDBObject => Box

  def getTraversable[A](field:String)(implicit dbo: MongoDBObject): Traversable[A] = {
    asDBList(field)(dbo).map (
      _.asInstanceOf[Traversable[A]]
    ).getOrElse(
      Seq[A]()
    )
  }

  def userBox(implicit dbo: MongoDBObject, format: Format): Box = {
    val emails = (asDBList("emails") map { emails =>
      for {
        email <- emails.asInstanceOf[Traversable[DBObject]]
      } yield {
        emailSummary(new MongoDBObject(email))
      }
    }).getOrElse(Seq(nullBox))

    def starredDocs(f: BoxerFn): List[Box] = (for {
      duuid <- getTraversable[UUID]("starredDocuments")
      dobj <- documents.find(withUUID(duuid))
    } yield f(dobj)).toList

    def emailBox = vjoin(center1)(tbox("         emails         ") % vjoin()(
      emails.toList:_*)) |> borderInlineTop

    def starredBox = vjoin(center1)(
      tbox("  starred  "), 
      vjoin()(
        starredDocs(
          docBox(_, ShortDescription)
        ):_*
      )
    ) |> borderInlineTop

    def starredBox2 = vjoin(center1)(tbox("  starred  "), vjoin()(
      starredDocs(docBox(_, FullDescription)):_*)) |> borderInlineTop

    format match {
      case FullDescription => recordBox +| valBox[String]("username")
      case ShortDescription => recordBox
      case Summary  =>
        vjoin()(
          recordBox,
          indent()(vjoin()(
            // epIcon(Iconic1),
            (emailBox +| starredBox))))
    }
  }

  def indent(n:Int=4)(b:Box): Box = {
    emptyBox(1)(n) + b
  }


  def revisionChain(n:Int=10)(implicit dbo: MongoDBObject):Box = {
    (for {
      id <- getAsUUID("revisionOf") if n > 0
      doc <- find(documents)(id)
    } yield {
      docBox(doc, ShortDescription) atop revisionChain(n-1)(doc)
    }) getOrElse (
      nullBox
    )
  }

  def responseChain(n:Int=10)(implicit dbo: MongoDBObject):Box = {
    (for {
      id <- getAsUUID("inResponseTo") if n > 0
      doc <- find(documents)(id)
    } yield {
      docBox(doc, ShortDescription) atop responseChain(n-1)(doc)
    }) getOrElse (
      nullBox
    )
  }

  def getDocumentBox(s:String)(implicit dbo: MongoDBObject): Option[Box] =  
    getAsUUID(s) >>= find(documents) >>= (o => Some(docBox(o, FullDescription)))


  def docBox(implicit dbo: MongoDBObject, format: Format): Box = {
    recordBox
    // TODO: put back response/revision chain (as generic list-formatting)
//     val idbox = memoizedIdBox
//     val docHeaderH = hjoin()(valBox[String]("type"), tbox("  "), idbox)
//     val docHeaderV = vjoin(center1)(valBox[String]("type"), idbox)
// 
//     val summary = docHeaderH atop indent()(
//       getCreatorBox atop (indent()(
//         vjoin(center1)(
//           tbox("in response to"),
//           (responseChain())
//         ) besideS (
//           vjoin(center1)(
//             tbox("revision of"),
//             (revisionChain())
//           )
//         )
//       ))
//     )
// 
//     format match {
//       case Iconic1 => idbox
//       case Iconic2 => docHeaderV |> borderInlineTop
//       case Summary => recordBox // summary
//     }

  }

  def eventProcessorBox(implicit dbo: MongoDBObject, format: Format): Box = {
    recordBox
    // TODO: put back subscribers (as generic list-formatting)
    //    val idbox = memoizedIdBox
    //
    //    def userIcon(f: Format) = (getAsUUID("_id") >>= find(users) >>= (o => Some(userBox(o, f)))) |> boxOrMessage("(no user)")
    //
    //    def subscribers(f: BoxerFn): List[Box] = (for {
    //      duuid <- getTraversable[UUID]("subscribers")
    //      dobj <- eventprocessors.find(withUUID(duuid))
    //    } yield f(dobj)).toList
    //
    //
    //    def subscribersBox = vjoin(left)(tbox("subscribers"), vjoin()(
    //      subscribers(
    //        eventProcessorBox(_, Iconic1)
    //      ):_*)) |> borderInlineTop
    //

    //format match {
    //  case Iconic1 =>
    //    hjoin(sep=tbox("; "))(
    //      valBox[String]("name"),
    //      recordBox
    //    )
    //  case Description =>
    //    modelDescription
    //
    //  case Summary =>
    //    recordBox
    //}
  }


  def list(c: Traversable[DBObject])(implicit fmt:Format = Summary) {
    println("\n")
    c.foreach{ o => println(renderDbo(o, fmt)); println() }
  }

  def renderDbo(implicit dbo: MongoDBObject, fmt:Format): String = {
    render(mongoDboBox)
  }

  def mongoDboBox(implicit dbo: MongoDBObject, fmt:Format): Box = {
    dbo.getAs[String]("type").getOrElse("?") match {
      case "arXivDocument"
         | "urlDocument"
         | "plainTextDocument"
         | "endorsement"            => docBox

      case "statefulEventProcessor"
         | "basicMailingList"
         | "mutableMailingList"
         | "anonymizer"             => eventProcessorBox

      case "license"
         | "request"
         | "withdraw"
         | "acknowledge"
         | "decline"
         | "fulfill"
         | "carboncopy"
         | "identityLicense"
         | "forward"                => recordBox

      case "linkedAccount"          => linkedAccountBox 
      case "user"                   => userBox

      case "userPassword"           => recordBox
      case "token"                  => recordBox
      case "ssToken"                => recordBox
      case x                        => recordBox
    }
  }

  def modelDescription(implicit dbo: MongoDBObject, fmt:Format): String = {
    (dbo.getAs[String]("type").getOrElse("?") match {
      case "arXivDocument"
         | "urlDocument"
         | "plainTextDocument"
         | "endorsement"            => 
        val m = CasbahDocumentStore.documentFromDbOpt(dbo)
        m.map(_.title.map(quoted).getOrElse("")).getOrElse("")

      case s
          if s.endsWith("MailingList") || s.endsWith("EventProcessor") || s == "anonymizer" =>
        val m = CasbahEventProcessorStore.eventProcessorFromDbOpt(dbo)
        quoted(m.map(_.toString).getOrElse(""))

      case "license"
         | "request"
         | "withdraw"
         | "acknowledge"
         | "decline"
         | "fulfill"
         | "carboncopy"
         | "identityLicense"
         | "forward"                =>
        val m = CasbahEventStore.eventFromDbOpt(dbo)
        m.map(t=>quoted(t.subject)).getOrElse("")

      case "linkedAccount"          => "account"
      case "user"                   => 
        val m = CasbahUserStore.userFromDb(dbo)
        m.fullname.getOrElse(m.username)
      case "userPassword"           => "userpswd"
      case "token"                  => "token"
      case "ssToken"                => "sstoken"
      case x                        => "<?>"
    }).oneLine
  }
}
