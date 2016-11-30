package de.htw.ai.wikiplag.data

import com.mongodb.casbah.Imports._
import de.htw.ai.wikiplag.model.Document
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by chris on 23.11.2016.
  */
class MongoDbClient(sc: SparkContext,
                    createInverseIndexFct: () => MongoCollection,
                    createDocumentCollectionFct: () => MongoCollection) extends Serializable {

  lazy val documentsCollection: MongoCollection = createInverseIndexFct()
  lazy val inverseIndexCollection: MongoCollection = createDocumentCollectionFct()

  //  // Set up the configuration for reading from MongoDB.
  //  val mongoConfig = new Configuration()
  //  // MongoInputFormat allows us to read from a live MongoDB instance.
  //  // We could also use BSONFileInputFormat to read BSON snapshots.
  //  // MongoDB connection string naming a collection to read.
  //  // If using BSON, use "mapred.input.dir" to configure the directory
  //  // where the BSON files are located instead.
  //  mongoConfig.set("mongo.input.uri",
  //    "mongodb://localhost:27017/db.collection")
  //
  //  // Create an RDD backed by the MongoDB collection.
  //  val documents = sc.newAPIHadoopRDD(
  //    mongoConfig,                // Configuration
  //    classOf[MongoInputFormat],  // InputFormat
  //    classOf[Object],            // Key type
  //    classOf[BSONObject])        // Value type

  def getInvIndex(token: String): List[(Long, List[Int])] = {
    val entry = inverseIndexCollection.findOneByID(MongoDBObject("_id" -> token)).get

    entry.asInstanceOf[BasicDBObject].getAs[List[(Long, List[Int])]]("doc_list").get
  }

  private def parseInvIndex = (x: DBObject) =>  {
    val word = x.asInstanceOf[BasicDBObject].getString("_id")
    val docList = x.asInstanceOf[BasicDBObject]
      .getAs[List[(Long, List[Int])]]("doc_list")
      .getOrElse(List.empty[(Long, List[Int])])

    (word, docList)
  }

  def getInvIndex(tokens: Set[String]): Map[String, List[(Long, List[Int])]] = {
    inverseIndexCollection.find("_id" $in tokens)
      .toList
      .map(parseInvIndex)
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
      .map(parseInvIndex)
      .foldLeft[RDD[(String, List[(Long, List[Int])])]](emptyRDD) {
      (rdd, x) => {
        val tmpRDD = sc.parallelize(Array(x))
        rdd.intersection(tmpRDD)
      }
    }
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

  private def parseDocument = (x: BasicDBObject) => {
    new Document(lId = x.getLong("_id", Long.MinValue),
      sText = x.getString("text", ""),
      sTitle = x.getString("title", ""),
      viewInd = x.getAsOrElse[List[(Int, Int, Int)]]("view_index", List.empty[(Int, Int, Int)]))
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

  def getDocumentsRDD(list: Set[Long]) : RDD[Document] = {
    val dataIterator = documentsCollection.find("_id" $in list).toIndexedSeq

    val emptyRDD = sc.emptyRDD[Document]

    dataIterator
      .map(x => x.asInstanceOf[BasicDBObject])
      .map(parseDocument)
      .foldLeft[RDD[Document]](emptyRDD) {
      (rdd, x) => {
        val tmpRDD = sc.parallelize(Array(x))
        rdd.intersection(tmpRDD)
      }
    }
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

  def apply(sc: SparkContext): MongoDbClient = {

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

    new MongoDbClient(sc, createInverseIndexFct, createDocumentCollectionFct)
  }
}
