package de.htw.ai.wikiplag.connection

import com.mongodb.casbah.Imports._
import de.htw.ai.wikiplag.model.Document

/**
  * Created by chris on 13.11.2016.
  */
trait MongoDBClient {

  protected final val WIKI_DATABASE = "wikiplag"
  protected final val COLLECTION_DOCUMENTS = "documents"
  protected final val COLLECTION_INVERSE_INDEX = "inv_idx"

  protected var mongoClient: MongoClient = _
  protected var documentsCollection: MongoCollection = _
  protected var inverseIndexCollection: MongoCollection = _

  /**
    * Open the Connection, with default values (HTW-Berlin)
    *
    * @param serverAddress MongoDB Server Address
    * @param credentials   Credentials for Login
    *
    */
  def open(serverAddress: ServerAddress, credentials: MongoCredential)

  /**
    * Close the Connection
    */
  def close()

  def getInvIndex(word: String): List[(Long, List[Int])]

  def getInvIndex(words: Set[String]): Map[String, List[(Long, List[Int])]]

  def getDocument(doc_id: Long): Document

  def getDocuments(list: Set[Long]): List[Document]


}
