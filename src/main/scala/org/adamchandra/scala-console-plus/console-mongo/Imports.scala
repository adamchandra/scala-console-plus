package net.openreview.model.raw.casbah.admin

import scala.Option
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
trait Imports extends MongoContext {


  val helpExamples = """

Switch to database
  :use my-database

Show all databases
  :dbs



Set verbosity with :v (verbose) or :q (quiet)
  The console output sometimes inadvertently eats error messages, 
  so use ":v" if some command silently fails 

Collection names are:
  events, eventprocessors, documents, users, tokens, linkedaccounts, venues, messagegenerators

Queries: 

  Collections are traversable, so that, e.g., 
    > eventprocessors

    will page through all records in that collection

  Run find over collection: 
    > users.find("username" $eq "saunders")  
    > users.find("username" $like "SaU")  <- $like runs case-insensitive regex substring match query


  The query operator '~' is a synonym for "$like", but with an operator 
    precedence that allows for omission of dots and parens, like so: 

     > users find "username" ~ "s(a|o)"


  The following are equivalent:
    > events.find("subject" $like "iclr")
    > events find "subject" ~ "iclr"
    > events.find( MongoDBObject("subject" -> 
                   MongoDBObject("$regex" -> rhs, 
                                 "$options" -> "i")))


Accessing openreview.model._ objects:
  See UUID operations below.
    > 29.uuid.(event|eventproc|document) 

Updating/Deleting records (Zippers):
  A zipper is wrapper around a mongo DBObject that allows traversal, get/set for fields, and saving changes.
  Its use is as follows: 

    Construct a zipper
      > val objZipper = 34.uuid.zipper

    Traverse to a field
      > 34.uuid.zipper.zipto("authors")

    get/set a field
      > objZipper.zipto("authors").get
      > objZipper.zipto("authors").set(newAuthorsUUID)

    Set and commit changes
      > objZipper.zipto("authors").set("92ac3413-0a89-4a53-baf7-421dfa3b4f2b".uuid).save

    Traverse deep structure
      > 10.uuid.zipper.zipto("requestTemplate").zipto("licenseTemplate").zipto("creator").set(27.uuid).save

    Deleting an object: 
      > 10.uuid.findOne.remove
        or
      > eventUUID.findOne.remove(events)

      If a collection is specified as an argument to  remove(), then only that collection will be searched. 
      If no collection is specified, all collections will be searched for the object to remove.


Standard Tasks:

   findSimilarUsers()
     find users with the same name (for identifying duplicate users)

   removeComment(commentUUID: UUID, dryRun:Boolean=true)
     Find and remove the comment w/given UUID, including related events, and update referring documents
     as necessary. dryRun=true (default) just lists items that will be deleted/modified.  

     Warning: This function will *not* necessarily do the right thing in the case of a comment that has responses.
       It is intended to delete misplaced or erroneously posted 'dangling' comments


Working with UUIDs:
   Convert a string to a UUID: 
     > "92ac3413-0a89-4a53-baf7-421dfa3b4f2b".uuid

   All UUIDs in the console have a number beside them like so: [37 @ 9a4a539a-7880-420c-be9f-6b1860569300]. That number 
     can be used as a shorthand to lookup its UUID like so: 
       > 37.uuid 
  
   By default, if the console value returns a UUID (or sequence of UUIDs), they will be looked up and displayed

   Operations on UUIDs include:
      find()      : find mongo records w/given UUID as "_id"
      findOne()   : same as find.headOption.getOrElse(error)
      findAny()   : search all collections for objects with the specified UUID in *any* field 
      raw()       : A "raw" representation of a mongo record, which includes UUID/BinData translations
      zipper()    : construct a "zipper" class, for traversing, updating, and saving changes to a record. See Zipper section for usage.

    Model accessors: returns the mongo object wrapped in the appropriate model object and cast to correct (base) instance.
                     Further casting might be necessary, e.g., 23.uuid.event.asInstanceOf[Request]
      event()      
      eventproc() 
      document()  

   

Server-side javascript:
    Various javascript functions must be stored on the server before many of the functions in the console
    will work. Server-side javascript is stored per-db in a collection called system.js. 

    On admin shell startup, and whenever a new database is selected with :use, the shell will attempt to 
    update the system.js collection in the current db. This uses an out-of-process call to mongo, so it will fail if mongo
    is not on the path. It will also fail if it can't find the .js file containing the server-side
    javascripts. This will happen if the shell is run from a jar file, rather than from an hg-checkout with the js sources available.
    This will only create a problem if javascript has changed since last updating a particular database.
      
"""

  implicit def str2ops = CasbahOps(_)

  trait CasbahOpsLow {
    def lhs: String
    def $eq(v:String) = MongoDBObject(lhs -> v)
    def $eq(uuid:UUID) = MongoDBObject(lhs -> uuid)
    def $equid(uuids:String) = MongoDBObject(lhs -> str2uuid(uuids))

    def str2uuid(s:String) = UUID.fromString(s)

    def $like(rhs:String): MongoDBObject = 
      MongoDBObject(lhs -> 
        MongoDBObject("$regex" -> rhs, "$options" -> "i"))


    def ~(lhs:String) = $like(lhs)

    def clip(maxlen:Int=80) = clipString(lhs, maxlen)
    def oneLine() = mkOneLine(lhs)
  }

  case class CasbahOps(
    override val lhs:String
  ) extends CasbahOpsLow

  def clipString(str:String, maxlen:Int=80): String = 
    str.substring(0, math.min(str.length, maxlen)) + (if (str.length > maxlen) "..." else "")

  def quoted(s:String) = "\""+s+"\""
  def quoted(n:NonemptyString): String = "\""+n.s+"\""

  def mkOneLine(s:String) = s.replaceAll("[\n\r\t\\s]+", " ")

  var mongoContext = EasyMongoContext("localhost", "openreview-devdb")
  def conn            : MongoConnection  = mongoContext.conn          
  def db              : MongoDB          = mongoContext.db            
  def events          : MongoCollection  = mongoContext.events        
  def eventprocessors : MongoCollection  = mongoContext.eventprocessors
  def documents       : MongoCollection  = mongoContext.documents     
  def users           : MongoCollection  = mongoContext.users         
  def userpasswords   : MongoCollection  = mongoContext.userpasswords 
  def tokens          : MongoCollection  = mongoContext.tokens        
  def linkedaccounts  : MongoCollection  = mongoContext.linkedaccounts
  def venues  : MongoCollection  = mongoContext.venues
  def messagegenerators  : MongoCollection  = mongoContext.messagegenerators

  // Server-side javascript storage (mongo documentation says use with great caution)
  def systemJs  : MongoCollection  = mongoContext.db("system.js")


  def setMongoContext(host:String, dbname:String) {
    mongoContext = EasyMongoContext(host, dbname)
  }


  def getAsUUID(field:String)(implicit dbo: MongoDBObject): Option[UUID] = 
    dbo.getAs[UUID](field)

  def getDateTime(field:String)(implicit dbo: MongoDBObject) = 
    dbo.getAs[DateTime](field)

  // def getDate(field:String)(implicit dbo: MongoDBObject) = dbo.getAs[java.util.Date](field)

  def getUUID(field:String)(implicit dbo: MongoDBObject):UUID = 
    dbo.getAs[UUID](field).getOrElse(sys.error("required uuid not found in MongoDBObject: "+dbo))

}
