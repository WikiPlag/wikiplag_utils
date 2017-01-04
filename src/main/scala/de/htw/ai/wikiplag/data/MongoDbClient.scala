package de.htw.ai.wikiplag.data

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCredential
import de.htw.ai.wikiplag.model.Document
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.BSONObject

/**
  * Created by chris on 23.11.2016.
  */
class MongoDbClient(sc: SparkContext,
                    InverseIndexHadoopConfig: Configuration,
                    DocumentsHadoopConfig: Configuration,
                    createInverseIndexFct: () => MongoCollection,
                    createDocumentCollectionFct: () => MongoCollection) extends Serializable {

  lazy val documentsCollection: MongoCollection = createDocumentCollectionFct()
  lazy val inverseIndexCollection: MongoCollection = createInverseIndexFct()

  def getInvIndex(token: String): List[(Long, List[Int])] = {
    val queryObject = MongoDBObject("_id" -> token)
    val entry = inverseIndexCollection.findOneByID(queryObject("_id")).orNull

    if (entry == null)
      List.empty[(Long, List[Int])]

    entry.asInstanceOf[BasicDBObject].getAs[List[(Long, List[Int])]]("doc_list").get
  }

  private def parse2InvIndexEntry(x: DBObject): (String, List[(Long, List[Int])]) = {
    val word = x.asInstanceOf[BasicDBObject].getString("_id")
    val docList = x.asInstanceOf[BasicDBObject].get("doc_list").asInstanceOf[BasicDBList]
    val scalaTypeList = docList.map(z => {
      val entry = z.asInstanceOf[BasicDBList]
      val id = entry.get(0).asInstanceOf[Long]
      val l = entry.get(1).asInstanceOf[BasicDBList].map(x => x.asInstanceOf[Int]).toList
      (id, l)
    }).toList
    (word, scalaTypeList)
  }

  private def parseFromInverseIndexEntry = (token: String, documents: List[(Long, List[Int])]) => {
    MongoDBObject(
      ("_id", token),
      ("doc_list", documents)
    )
  }

  def getInvIndex(tokens: Set[String]): Map[String, List[(Long, List[Int])]] = {
    inverseIndexCollection.find("_id" $in tokens)
      .toList
      .map(parse2InvIndexEntry)
      .toMap[String, List[(Long, List[Int])]]
  }

  def getInvIndexRDD(tokens: Set[String]): RDD[(String, List[(Long, List[Int])])] = {
    /**
      * What we do here is:
      * 1. fetching data from mongoDB and handle the data by a native scala iterator (toIndexedSeq)
      * 2. go through the iterator and parse every db entry to our required inverse index data structure of (String - document list)
      * 3. accumulating a big RDD.
      */
    val dataIterator = inverseIndexCollection.find("_id" $in tokens).toIndexedSeq

    val emptyRDD = sc.emptyRDD[(String, scala.List[(Long, scala.List[Int])])]

    dataIterator
      .map(parse2InvIndexEntry)
      .foldLeft[RDD[(String, List[(Long, List[Int])])]](emptyRDD) {
      (rdd, x) => {
        val tmpRDD = sc.parallelize(Array(x))
        rdd.union(tmpRDD)
      }
    }
  }

  def insertInverseIndex(inverseIndex: Map[String, List[(Long, List[Int])]]): Unit = {
    val entries = inverseIndex.map(x => parseFromInverseIndexEntry(x._1, x._2))
    entries.foreach(x => inverseIndexCollection.insert(x))
  }

  //  def sampleFunction(): Unit = {
  //    //    val sparkSession = SparkSession.builder().getOrCreate()
  //    val builder = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0))
  //    val readConfig = builder.build()
  //    //    val mongoRDD = sparkSession.sqlContext.fromMongoDB(readConfig)
  //    //    mongoRDD.createTempView("students")
  //    //    val dataFrame = sparkSession.sql("SELECT name, age FROM students")
  //    //    dataFrame.show
  //    val sqlContext = new SQLContext(sc)
  //    val mongoDF = sqlContext.fromMongoDB(readConfig)
  //    mongoDF.registerTempTable("documents")
  //    val documentsRDD = sqlContext.sql("SELECT * from documents")
  //
  //
  //  }

  private def parseFromDocument = (d: Document) => {
    MongoDBObject(
      ("_id", d.id),
      ("title", d.title),
      ("text", d.text),
      ("viewindex", d.viewIndex)
    )
  }

  def getDocument(doc_id: Long): Document = {
    val queryObject = MongoDBObject("_id" -> doc_id)
    val documentEntry = documentsCollection.findOneByID(queryObject("_id")).get

    val monogoDbObject = documentEntry.asInstanceOf[BasicDBObject]

    MongoDbClient.parse2Document(monogoDbObject)
  }

  def getDocuments(list: Set[Long]): List[Document] = {
    documentsCollection.find("_id" $in list).toList
      .map(x => x.asInstanceOf[BasicDBObject])
      .map(MongoDbClient.parse2Document)
  }

  def getAllDocumentsRDD: RDD[Document] = {
    //    val sb:StringBuilder = new StringBuilder().append('[')
    //    list.foreach(x => sb.append(x).append(','))
    //    // replace last ',' with ']'
    //    sb.replace(sb.size - 1, sb.size, "]")
    //
    //    DocumentsHadoopConfig.set("input.mongo.query", "{_id:{ $in :" + sb.toString + "}")

    val documents = sc.newAPIHadoopRDD(DocumentsHadoopConfig, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object], classOf[BSONObject])

    documents
      .map(_._2.asInstanceOf[BasicDBObject])
      .map(x => MongoDbClient.parse2Document(x))
//      .filter(x => list.contains(x.id))
  }

  def getDocumentsRDD(list: Set[Long]): RDD[Document] = {
        val dataIterator = documentsCollection.find("_id" $in list).toIndexedSeq

        val emptyRDD = sc.emptyRDD[Document]

        dataIterator
          .map(x => x.asInstanceOf[BasicDBObject])
          .map(MongoDbClient.parse2Document)
          .foldLeft[RDD[Document]](emptyRDD) {
          (rdd, x) => {
            val tmpRDD = sc.parallelize(Array(x))
            rdd.union(tmpRDD)
          }
        }
  }

  def insertDocument(document: Document): Unit = {
    documentsCollection.insert(parseFromDocument(document))
  }

  def insertDocument(documents: Set[Document]): Unit = {
    val entries = documents.map(parseFromDocument)
    entries.foreach(x => documentsCollection.insert(x))
  }
}

object MongoDbClient {

  protected final val WIKI_DATABASE = "wikiplag"
  protected final val COLLECTION_DOCUMENTS = "documents"
  protected final val COLLECTION_INVERSE_INDEX = "inv_idx"

  protected var serverAddress: ServerAddress = _
  protected var credentials: List[MongoCredential] = _

  private def parse2Document = (x: BasicDBObject) => {
    new Document(id = x.getLong("_id", Long.MinValue),
      text = x.getString("text", ""),
      title = x.getString("title", ""),
      viewIndex = x.getAsOrElse[List[(Int, Int, Int)]]("viewindex", List.empty[(Int, Int, Int)]))
  }

  def open(): MongoClient = {
    //    val serverAddress: ServerAddress = new ServerAddress(ServerAddress, SERVER_PORT)
    //    val credentials: List[MongoCredential] = List(MongoCredential.createCredential(Username, Database, Password.toCharArray))

    MongoClient(serverAddress, credentials)
  }

  def apply(sc: SparkContext, host: String, port: Integer, dbName: String, user: String, password: String): MongoDbClient = {
    serverAddress = new ServerAddress(host, port)
    credentials = List(MongoCredential.createCredential(user, dbName, password.toCharArray))

    val uriBuilder = new StringBuilder("mongodb://").append(new String(host)).append(":").append(String.valueOf(port))
      .append("/").append(dbName).append('.')
    val authUriBuilder = new StringBuilder("mongodb://").append(user).append(":")
      .append(password).append("@").append(host).append(":").append(port).append('/')
      .append(dbName)
    val inputUriDocuments = uriBuilder.toString() + COLLECTION_DOCUMENTS
    val inputUriInvIndex = uriBuilder.toString() + COLLECTION_INVERSE_INDEX
    val authUri = authUriBuilder.toString()

    val documentsConfig = new Configuration()
    documentsConfig.set("mongo.input.uri", inputUriDocuments)
    documentsConfig.set("mongo.auth.uri", authUri)
    val invIndexConfig = new Configuration()
    invIndexConfig.set("mongo.input.uri", inputUriInvIndex)
    invIndexConfig.set("mongo.auth.uri", authUri)


    val createInverseIndexFct = () => {
      val mongoClient = MongoClient(serverAddress, credentials)

      sys.addShutdownHook {
        mongoClient.close()
      }

      mongoClient(WIKI_DATABASE)(COLLECTION_INVERSE_INDEX)
    }

    val createDocumentCollectionFct = () => {
      val mongoClient = MongoClient(serverAddress, credentials)

      sys.addShutdownHook {
        mongoClient.close()
      }

      mongoClient(WIKI_DATABASE)(COLLECTION_DOCUMENTS)
    }

    new MongoDbClient(sc, invIndexConfig, documentsConfig, createInverseIndexFct, createDocumentCollectionFct)
  }
}
