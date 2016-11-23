package de.htw.ai.wikiplag.data

import com.mongodb.casbah.Imports._
import de.htw.ai.wikiplag.model.Document

/**
  * Created by chris on 23.11.2016.
  */
class MongoDbClient(createInverseIndexFct: () => MongoCollection,
                    createDocumentCollectionFct: () => MongoCollection) extends Serializable {

  lazy val documentsCollection: MongoCollection = createInverseIndexFct()
  lazy val inverseIndexCollection: MongoCollection = createDocumentCollectionFct()

  def getInvIndex(word: String): List[(Long, List[Int])] = {
    val entry = inverseIndexCollection.findOneByID(MongoDBObject("_id" -> word)).get

    entry.asInstanceOf[BasicDBObject].getAs[List[(Long, List[Int])]]("doc_list").get
  }

  def getInvIndex(words: Set[String]): Map[String, List[(Long, List[Int])]] = {
    inverseIndexCollection.find("_id" $in words)
      .toList
      .map(x => {
        val word = x.asInstanceOf[BasicDBObject].getString("_id")
        val docList = x.asInstanceOf[BasicDBObject]
          .getAs[List[(Long, List[Int])]]("doc_list")
          .getOrElse(List.empty[(Long, List[Int])])

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

  private val parseDocument = new Function[BasicDBObject, Document]{
    def apply(x : BasicDBObject) : Document = {
      new Document(lId = x.getLong("_id", Long.MinValue),
        sText = x.getString("text", ""),
        sTitle = x.getString("title", ""),
        viewInd = x.getAsOrElse[List[(Int, Int, Int)]]("view_index", List.empty[(Int, Int, Int)]))
    }
  }

  def getDocument(doc_id: Long): Document = {
    val documentEntry = documentsCollection.findOneByID(MongoDBObject("_id" -> doc_id)).get

    val monogoDbObject = documentEntry.asInstanceOf[BasicDBObject]

    parseDocument(monogoDbObject)
  }

  def getDocuments(list: Set[Long]): List[Document] = {
    documentsCollection.find("_id" $in list).toList
      .map(x => x.asInstanceOf[BasicDBObject])
      .map(parseDocument)
  }
}

object MongoDbClient {

  protected final val WIKI_DATABASE = "wikiplag"
  protected final val COLLECTION_DOCUMENTS = "documents"
  protected final val COLLECTION_INVERSE_INDEX = "inv_idx"

  val SERVER_PORT = 27020
  val ServerAddress = "hadoop03.f4.htw-berlin.de"
  val Password = "REPLACE-ME"
  val Database = "REPLACE-ME"
  val Username = "REPLACE-ME"

  def open(): MongoClient = {
    val serverAddress: ServerAddress = new ServerAddress(ServerAddress, SERVER_PORT)
    val credentials: List[MongoCredential] = List(MongoCredential.createCredential(Username, Database, Password.toCharArray))

    MongoClient(serverAddress, credentials)
  }

  def apply(): MongoDbClient = {

    val createInverseIndexFct = () => {
      val mongoClient = open()

      sys.addShutdownHook {
        mongoClient.close()
      }

      mongoClient(WIKI_DATABASE)(COLLECTION_INVERSE_INDEX)
    }

    val createDocumentCollectionFct = () => {
      val mongoClient = open()

      sys.addShutdownHook {
        mongoClient.close()
      }

      mongoClient(WIKI_DATABASE)(COLLECTION_DOCUMENTS)
    }

    new MongoDbClient(createInverseIndexFct, createDocumentCollectionFct)
  }
}
