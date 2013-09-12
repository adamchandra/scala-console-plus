package net.openreview.model.raw.casbah.admin

import scala.Option
import java.util.UUID
import net.openreview.model.raw._
import net.openreview.model.raw.casbah._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import scalaz.syntax.id._
import scalaz.syntax.monad._
import scalaz.std.function._
import scalaz.syntax.traverse._
import scalaz.ValidationNel
import scalaz.syntax.std.option._
import scalaz.std.option._
import scalaz.std.list._
import scalaz.Validation, Validation._, scalaz.syntax.validation._
import net.openreview.model.admin.IntegrityTest
import net.openreview.model.users.User


/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait Queries extends Imports {

  def installSystemJavascripts() {
    try {
      import sys.process._
      println("updating server-side javascript")
      val js = "./prj-openreview-core/src/main/javascript/serverJavascript.js";
      val install = "./prj-openreview-core/src/main/javascript/installJs.js";
      ("mongo " + db.name + " " + js + " " + install).!
    } catch {
      case _ : Throwable => println("error installing system javascript")
    }
  }

  def testSSJavascript() {
    evalJavascript("hello()")
  }

  def evalJavascript(func: String, args: String*): Validation[String, Object] = {
    val result = db.command(Map(
      "eval" -> func,
      "args" -> args.toList
    ));

    if (result.ok) {
      result.get("retval").success
    } else {
      result.getErrorMessage().failure
    }
  }

  case class StringOutput(s: String)

  def rawFormat(id: UUID): StringOutput = {
    (for {
      r <- evalJavascript("function (juuid) { return objectToString(findByJuuid(juuid)); }", id.toString)
    } yield {
      r.toString()
    }).fold({
      err => StringOutput(err)
    }, {
      ok => StringOutput(ok)
    })
  }

  def getCollectionNames(): List[String] = {
    evalJavascript("function () { return db.getCollectionNames(); }").fold(
      f => List("error"),
      succ => succ.asInstanceOf[BasicDBList].map(_.asInstanceOf[String]).toList
    )
  }

  def withUUID(id: UUID) = "_id" $eq id


  def findAnyWithUUID(id: UUID): Seq[UUID] = {
    val mapJS = """
     function(){
        if(hasUUID(this, '%s')) {
            emit(binToJUUID(this._id), 1);
        }
     }
                """.format(id.toString)

    val reduceJS = """ function(key, values){ return 0; } """

    for {
      coll <- collections
      results = coll.mapReduce(mapJS, reduceJS, MapReduceInlineOutput, verbose = true)
      _ = if (results.isError) {
        results.err foreach {
          println(_)
        }
      }
      if !results.isError
      r <- results.toSeq
      idstr <- r.getAs[String]("_id")
    } yield {
      UUID.fromString(idstr.stripPrefix("{").stripSuffix("}"))
    }

  }

  def find(col: MongoCollection): UUID => Option[MongoDBObject] =
    uid => for (o <- col.findOne(withUUID(uid))) yield o

  // todo check for duplicate email addresses too

  def findSimilarUsers() {
    val mapJS = """
    function(){
       if(this.fullname) {
            emit(this.fullname.toLowerCase(), 1);
       }
    }
                """

    val reduceJS = """
    function(key, values){
        var result = 0;
        values.forEach(function(value) {
          result += value;
        });
        return result;
    }
                   """

    val finalizeJS = """
    function isDuplicated(element, index, array) {
      return (element.value > 1);
    }
                     """

    val results = users.mapReduce(mapJS, reduceJS,
      MapReduceInlineOutput,
      // finalizeFunction=Some(finalizeJS),
      verbose = true
    )

    if (results.isError) {
      results.err foreach {
        println(_)
      }
    } else {
      results.foreach {
        r =>
          if (r.getAs[Double]("value").cata(
            (_ > 1.0), false
          )) {
            println(r)
          }

      }
    }
  }


  import scala.collection.mutable

  val uuid2i = mutable.HashMap[UUID, Int]()
  val i2uuid = mutable.HashMap[Int, UUID]()
  var _curi = 0

  def memoUUID(uuid: UUID): Int = {
    if (!uuid2i.contains(uuid)) {
      uuid2i(uuid) = _curi
      i2uuid(_curi) = uuid
      _curi += 1
    }
    uuid2i(uuid)
  }

  def clearMemo() {
    uuid2i.clear()
    i2uuid.clear()
    _curi = 0
  }


  lazy val collections = Seq(events, eventprocessors, documents, users, tokens, linkedaccounts, venues, 
    messagegenerators)


  def all(f: (MongoCollection) => Iterable[DBObject]): Iterable[DBObject] = {
    for {
      coll <- collections
      o <- f(coll)
    } yield o
  }


  implicit def StringToOps(s: String) = new StringOps {
    val str = s
  }
  implicit def IntToIntQueryOps(n: Int) = new IntQueryOps {
    val i = n
  }
  implicit def UUIDToOpt(u: UUID) = new UUIDQueryOps {
    def _uuid: UUID = u
  }
  implicit def DboToOpts(u: DBObject) = new DBObjectOps {
    def _dbo: DBObject = u
  }

  trait IntQueryOps {
    def i: Int
    def uuid = uuidfor(i)
  }


  trait StringOps {
    def str: String
    def uuid = UUID.fromString(str)
  }

  trait UUIDQueryOps {
    def _uuid: UUID
    def find(): Iterable[DBObject] = findById(_uuid)
    def findOne(coll: Option[MongoCollection] = None): DBObject = findById(_uuid, 
      coll).headOption.getOrElse(sys.error("findOne() did not return a result"))
    def findOne(): DBObject = findById(_uuid).headOption.getOrElse(sys.error("findOne() did not return a result"))
    def findAny(): Seq[UUID] = findAnyWithUUID(_uuid)
    def raw(): StringOutput = rawFormat(_uuid)
    def event(): Event = findOne().event()
    def eventproc(): EventProcessor = findOne().eventproc()
    def document(): Document = findOne().document()
    def user(): User = findOne(Some(users)).user()
    def venue(): Venue = findOne().venue()
    def messagegenerator: MessageGenerator = findOne().messagegenerator()
    def zipper(): DboZipperRoot = find.head.zipper
  }

  // Map[String, (DBObject => )]
  // TODO: make models easily accessible
  trait DBObjectOps {
    def _dbo: DBObject

    def event(): Event = {
      CasbahEventStore.eventFromDbOpt(_dbo).getOrElse(sys.error("no model available"))
    }

    def eventproc(): EventProcessor = {
      CasbahEventProcessorStore.eventProcessorFromDbOpt(_dbo).getOrElse(
        CasbahEventProcessorStore.statefulEventProcessorFromDbOpt(_dbo).getOrElse(
          sys.error("no model available")))
    }

    def document(): Document = {
      CasbahDocumentStore.documentFromDbOpt(_dbo).getOrElse(sys.error("no model available"))
    }

    def user(): User = {
      CasbahUserStore.userFromDb(_dbo) //.getOrElse(sys.error("no model available"))
    }

    def venue(): Venue = {
      CasbahVenueStore.venueFromDbOpt(_dbo).getOrElse(sys.error("no model available"))
    }

    def messagegenerator(): MessageGenerator = {
      CasbahMessageGeneratorStore.messageGeneratorFromDbOpt(_dbo).getOrElse(sys.error("no model available"))
    }

    def zipper(): DboZipperRoot = {
      DboZipperRoot(_dbo) //Some(_dbo))
    }

    def save(): Validation[String, MongoDBObject] = {
      val uuid = getUUID("_id")(_dbo)
      collectionForId(uuid).flatMap(save(_)) //fold(e=>e,c=>save(c))
    }

    def save(coll: MongoCollection): Validation[String, MongoDBObject] = {
      val uuid = getUUID("_id")(_dbo)
      coll.save(_dbo)
      find(coll)(uuid).toSuccess("saved DBObject not found, uuid=" + uuid)
      /*for {
        coll <- coll.toSuccess("never fails").orElse(collectionForId(uuid))
        _ <- coll.save(_dbo)
        updated <- find(coll)(uuid).toSuccess("saved DBObject not found, uuid="+uuid)
      } yield {
        updated
      }*/
    }

    // avoid implicit naming collision
    def delete() = remove()
    def delete(coll: MongoCollection): Validation[String, Unit] = remove(coll)

    def remove(): Validation[String, Unit] = {
      val uuid = getUUID("_id")(_dbo)
      collectionForId(uuid).flatMap(remove(_)) //fold(e=>e,c=>remove(c))
    }

    def remove(coll: MongoCollection): Validation[String, Unit] = {
      val uuid = getUUID("_id")(_dbo)
      coll.remove(DBObject("_id" -> uuid))
      ().success
      /*for {
        coll <- coll.toSuccess("never fails").orElse(collectionForId(uuid))
        _ <- coll.remove(DBObject("_id" -> uuid))
      } yield {
        ()
      }*/
    }
  }

  def uuidfor(n: Int): UUID = {
    i2uuid.get(n).getOrElse(sys.error("no uuid for @" + n))
  }

  def findById(i: Int): Iterable[DBObject] = {
    findById(uuidfor(i))
  }


  def findById(uuid: UUID, collopt: Option[MongoCollection] = None): Iterable[DBObject] = {
    for {
    // if the UUID we're looking for is a dangling reference, then the collectionForId search for it will fail.
    // still we want to see where the problem came from, so don't throw an error.
      coll <- collopt.cata(
        some = c => List(c),
        none = collectionForId(uuid).fold(err => List(), //sys.error(err),
          c => List(c))
      )
      o <- coll.findOne(withUUID(uuid))
    } yield o
  }

  def collectionForId(uuid: UUID): Validation[String, MongoCollection] = {
    (for {
      coll <- collections
    } yield {
      coll.findOne(withUUID(uuid)) -> coll
    }).filter(_._1 != None).map(_._2).headOption.toSuccess("no collection for record w/uuid:" + uuid)
  }


  trait DboZipper {
    def hole: Any
    def root: DboZipperRoot
    def get[T] = hole.asInstanceOf[T]
  }

  trait DboZipperNotLeaf
    extends DboZipper {
    def save(): Validation[String, MongoDBObject]
    override def hole: DBObject

    def zipto(field: String): DboZipperNotRoot = {
      if (hole.containsField(field)) {
        hole.get(field) match {
          case dbo: DBObject => DboZipperInternal(this, field, dbo)
          case x => DboZipperLeaf(this, field, x)
        }
      } else {
        DboZipperLeaf(this, field, None)
      }
    }
  }

  trait DboZipperNotRoot extends DboZipper {
    def parent: DboZipperNotLeaf
    def field: String

    def root: DboZipperRoot = parent.root
    def save(): Validation[String, MongoDBObject] = parent.save()
    
    def remove: DboZipperNotLeaf = {
      parent.hole.removeField(field)
      parent
    }

    def mod[T: Manifest](f: T => T): DboZipperNotRoot = set(f(get[T]))

    def set(v: Any): DboZipperNotRoot = {
      // I'm confused how this is really functional when we mutate the actual storage, but anyway.
      parent.hole.put(field, v)

      v match {
        case dbo: DBObject => DboZipperInternal(parent, field, dbo)
        case x => DboZipperLeaf(parent, field, Some(x))
      }
    }
  }

  case class DboZipperRoot(
                            override val hole: DBObject
                            ) extends DboZipperNotLeaf {
    def root = this
    def save(): Validation[String, MongoDBObject] = hole.save()   
  }

  case class DboZipperLeaf(
                            override val parent: DboZipperNotLeaf,
                            override val field: String,
                            override val hole: Any
                            ) extends DboZipperNotRoot

  case class DboZipperInternal(
                                override val parent: DboZipperNotLeaf,
                                override val field: String,
                                override val hole: DBObject
                                ) extends DboZipperNotRoot with DboZipperNotLeaf


  // TODO memoize a few of the UUID, Collection, DBObject lookups to make subsequent lookups faster

  def fieldEquals[T: Manifest](objId: UUID, field: String, v: T): Boolean = {
    objId.findOne.getAs[T](field).cata(_ == v, false)
  }

  def fieldFilter[T: Manifest](objId: UUID, field: String, test: T => Boolean): Boolean = {
    objId.findOne.getAs[T](field).cata(test(_), false)
  }

  def isEvent(id: UUID): Boolean = find(events)(id).isDefined
  def isDocument(id: UUID): Boolean = find(documents)(id).isDefined
  def isUser(id: UUID): Boolean = find(users)(id).isDefined


  def bfs(v: UUID, adjacentTo: UUID => List[UUID], verbose: Boolean = false): List[UUID] = {
    import scala.collection.mutable
    val q = mutable.Queue[UUID]()
    val marked = mutable.HashSet[UUID]()

    def mark(x: UUID) {
      marked += x
    };
    def isMarked(x: UUID): Boolean = marked.contains(x)

    q.enqueue(v)
    mark(v)
    while (!q.isEmpty) {
      if (verbose) {
        println("search state: open/closed sizes: " + q.size + ", " + marked.size)
      }
      val t = q.dequeue()
      for {
        u <- adjacentTo(t) if !isMarked(u)
      } {
        mark(u)
        q += u
      }
    }
    marked.toList
  }

  def bfspar(v: UUID, adjacentTo: UUID => List[UUID], verbose: Boolean = false, doubleCheckBFS: Boolean = false): 
  List[UUID] = {
    import scala.collection.mutable
    val q = mutable.Queue[UUID]()
    val marked = mutable.HashSet[UUID]()

    def mark(x: UUID) {
      marked += x
    };
    def isMarked(x: UUID): Boolean = marked.contains(x)

    def printSearchState(hdr: String) {
      if (verbose) {
        println("at: " + hdr)
        println("  open: " + q.mkString(", "))
        println("  closed: " + marked.mkString(", "))
      }
    }
    q.enqueue(v)
    mark(v)
    while (!q.isEmpty) {
      printSearchState("top")
      val currq = List(q: _*)
      q.clear()
      for (
        u <- currq.par.map(adjacentTo(_)).seq.flatten
        if !isMarked(u)
      ) {
        mark(u)
        q += u
      }

    }

    if (doubleCheckBFS) {
      val verifyMarked = bfs(v, adjacentTo, verbose).toSet
      if (marked.toSet == verifyMarked) {
        println("verified that parallel bfs == sequential bfs")
      } else {
        println("ERROR!: parallel bfs != sequential bfs")
        println(verifyMarked.mkString(", "))
      }
    }

    marked.toList
  }


  def removeComment(commentUUID: UUID, dryRun: Boolean = true): Map[String, Seq[UUID]] = {
    // include child comments w/inResponseTo==commentUUID
    val childDocs = (for {
      relatedDocId <- commentUUID.findAny
      if fieldFilter[String](relatedDocId, "type", (_.endsWith("Document"))) && fieldEquals[UUID](relatedDocId, 
        "inResponseTo", commentUUID)
    } yield {
      relatedDocId
    }).toSeq

    // includes requests, licenses
    val licenses = (for {
      relatedEventId <- commentUUID.findAny if isEvent(relatedEventId)
      if fieldFilter[String](relatedEventId, "type", (_ == "license"))
    } yield {
      relatedEventId
    }).toSeq

    // includes forwards/CCs of any related events (licenses in particular)
    val wrapped = (for {
      e <- licenses
      re <- e.findAny if isEvent(re) && fieldEquals[UUID](re, "wrapped", e)
    } yield re).toSeq

    val affectedIDs = Map[String, Seq[UUID]](
      "original" -> Seq(commentUUID),
      "childDocs" -> childDocs, // any child doc needs its inResponseTo removed
      "licenses" -> licenses,
      "wrapped" -> wrapped
    )

    if (!dryRun) {
      for {
        id <- affectedIDs("childDocs")
        doc <- find(documents)(id)
      } {
        doc.removeField("inResponseTo")
        doc.underlying.save(documents)
      }
      for {
        id <- affectedIDs("licenses")
        lic <- find(events)(id)
      } {
        lic.underlying.remove(events)
      }
      for {
        id <- affectedIDs("wrapped")
        ev <- find(events)(id)
      } {
        ev.underlying.remove(events)
      }

      for {
        id <- affectedIDs("original")
        comment <- find(documents)(id)
      } {
        comment.underlying.remove(documents)
      }
    }
    affectedIDs
  }


  def findAllRelatedRecords(rootId: UUID)(filterf: UUID => Boolean): List[UUID] = {
    def adj(id: UUID): List[UUID] = {
      id.findAny.filter(_ != id).filter(filterf).toList
    }
    bfs(rootId, adj, true).filter(filterf)
  }

  trait Transaction {
    def desc: String
    def commit: ValidationNel[String, Unit]
  }

  // TODO: make transaction functions use validation to accrue errors or success
  case class Transactions(
                           val desc: String,
                           ts: Transaction*
                           ) extends Transaction {
    override def commit: ValidationNel[String, Unit] = {
      val subTrans: List[ValidationNel[String, Unit]] = (ts.toList map (_.commit))
      val combinedTrans: ValidationNel[String, List[Unit]] = subTrans.sequenceU
      ().success
    }
  }

  case class TransactionList[T](
                                 val desc: String,
                                 ids: List[T],
                                 f: T => Unit
                                 ) extends Transaction {
    def commit: ValidationNel[String, Unit] = {
      ids.map(f(_))
      ().success
    }
  }


  def isWrapperFor(outerEv: UUID, innerEv: UUID): Boolean = {
    (for {
      e <- find(events)(outerEv).headOption
      wid <- e.getAs[UUID]("wrapped") if wid == innerEv
    } yield true).getOrElse(false)
  }

  // remove a top-level document or event
  def removeEntity(docId: UUID): Transaction = {
    val relatedEvents = findAllRelatedRecords(docId)(isEvent(_))
    val relatedUsers = findAllRelatedRecords(docId)(isUser(_))
    val relatedDocs = findAllRelatedRecords(docId)(isDocument(_))

    val userStarred = relatedUsers.toList.filter(
      fieldFilter[BasicDBList](_, "starredDocuments", (_.contains(docId))))

    val responsesTo = relatedDocs.toList.filter(
      fieldEquals[UUID](_, "inResponseTo", docId))

    val (ccsAndFwds, wrappedEvents) = (for (
      e1 <- relatedEvents; e2 <- relatedEvents
      if isWrapperFor(e1, e2)
    ) yield
      e1 -> e2
      ).foldLeft(Set[UUID]() -> Set[UUID]()) {
      case ((s1, s2), (e1, e2)) =>
        (s1 + e1) -> (s2 + e2)
    }

    val wrappedNonCcsFwds = wrappedEvents filterNot (ccsAndFwds.contains)
    val eventsWithoutCcsAndFwds = relatedEvents filterNot (ccsAndFwds.contains) filterNot (wrappedNonCcsFwds.contains)

    val removeDocumentTaxn = if (isDocument(docId)) List(TransactionList[UUID](
    "Document will be deleted", List(docId), {
      t =>
        t.findOne(documents.some).remove(documents)
    }))
    else Nil

    val allTaxns = removeDocumentTaxn ::: List(TransactionList[UUID](
    "These were wrapped in a cc or fwd, will be deleted", wrappedNonCcsFwds.toList, {
      t =>
        t.findOne(events.some).remove(events)
    }),
      TransactionList[UUID](
      "All events minus ccs/fwds, will be deleted", eventsWithoutCcsAndFwds.toList, {
        t =>
          t.findOne(events.some).remove(events)
      }),
      TransactionList[UUID](
      "All forwards/ccs will be deleted", ccsAndFwds.toList, {
        t =>
          t.findOne(events.some).remove(events)
      }),
      TransactionList[UUID](
      "responses to document will have 'inResponseTo field removed'", responsesTo, {
        t =>
          t.findOne(documents.some).zipper.zipto("inResponseTo").remove.save()
      }),
      TransactionList[UUID](
      "users w/this document starred will have it unstarred", userStarred, {
        t =>
          t.findOne(users.some).zipper.zipto("starredDocuments").mod(
          {
            (b: BasicDBList) => b.remove(docId); b
          }
          ).save()
      }))

    Transactions(
      "deleting a document: review the items that will be deleted/modified and then run .commit() on the transaction object. Also, plz run integrityTest() afterward (and perhaps before)",
      allTaxns: _*
    )
  }

  def printMap[K, V](kprint: K => String, vprint: V => String)(map: Map[K, V]): Unit = {
    println("{")
    for {
      k <- map.keys
    } {
      println("  " + kprint(k) + ": " + vprint(map(k)))
    }
    println("}")
  }

  def printUUIDMap(m: Map[UUID, List[UUID]]) = printMap[UUID, List[UUID]](_.toString, _.mkString(", "))(m)

  def integrityTest() {
    IntegrityTest.dbIsOK
  }
}
