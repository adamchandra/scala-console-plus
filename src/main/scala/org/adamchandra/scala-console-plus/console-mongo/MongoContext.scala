package org.adamchandra.sconsplus
package mongodb

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection



/*

 - Task list for Admin console
   - [x] Documentation, examples on startup and in :help

   - [x] Make sure all fields in all objects are displayed

   - [x] Make it clearer what type is being displayed (uuid, dbobject, list/traversable/iterable/etc)

   - [x] access to model objects

   - [ ] javascript server-side functions should register a function that reports on the particular version that is running 
         (just a time/datestamp that is created at upload time would be sufficient)

   - [ ] save specific dbobjects (push/pop/clear?) for later update/deletion

   - [ ] Create/send email from console

*/

trait MongoContext {
  def conn            : MongoConnection
  def db              : MongoDB
}

case class EasyMongoContext  (
  val host: String, val dbname:String
)extends MongoContext {
  lazy val conn            : MongoConnection  = MongoConnection(host)
  lazy val db              : MongoDB          = conn(dbname)
  // lazy val users           : MongoCollection  = db("user")
}










