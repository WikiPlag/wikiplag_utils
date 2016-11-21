package de.htw.ai.wikiplag.connection

import com.mongodb.casbah.Imports._
import de.htw.ai.wikiplag.connection.MongoDBImpl._
import de.htw.ai.wikiplag.model.Document

import scala.collection.immutable.HashMap

/**
  * Created by chris on 13.11.2016.
  */
object MongoDBClientImpl extends MongoDBClient {

  /**
    * Open the Connection, with default values (HTW-Berlin)
    *
    * @param serverAddress MongoDB Server Address
    * @param credentials   Credentials for Login
    *
    */
  override def open(serverAddress: ServerAddress, credentials: MongoCredential): Unit = {
    mongoClient = MongoClient(serverAddress, List(credentials))
    documentsCollection = mongoClient(WIKI_DATABASE)(COLLECTION_DOCUMENTS)
    inverseIndexCollection = mongoClient(WIKI_DATABASE)(COLLECTION_INVERSE_INDEX)
  }

  /**
    * Close the Connection
    */
  override def close(): Unit = {
    if (mongoClient != null) {
      mongoClient.close()
    }
  }

  override def getInvIndex(word: String): List[(Long, List[Int])] = {
    val entry = inverseIndexCollection.findOneByID(MongoDBObject("-id" -> word)).get

    entry.asInstanceOf[BasicDBObject].getAs[List[(Long, List[Int])]]("doc_list").get
  }

  override def getInvIndex(words: Set[String]): Map[String, List[(Long, List[Int])]] = {
    inverseIndexCollection.find("_id" $in words)
      .toList
      .map(x => {
        val word = x.asInstanceOf[BasicDBObject].getString("_id")
        val docList = x.asInstanceOf[BasicDBObject].getAs[List[(Long, List[Int])]]("doc_list")
        /**
          * .asInstanceOf[BasicDBList].toList
          * .map(viewIndexElement => (
          * viewIndexElement.asInstanceOf[BasicDBList].get(0).asInstanceOf[Int],
          * viewIndexElement.asInstanceOf[BasicDBList].get(1).asInstanceOf[Int],
          * viewIndexElement.asInstanceOf[BasicDBList].get(2).asInstanceOf[Int]
          * )
          * )
          * )
          */
        (word, docList)
      })
      .toMap[String, List[(Long, List[Int])]]
  }

  override def getDocument(doc_id: Long): Document = ???

  override def getDocuments(list: List[Long]): List[Document] = ???
}
