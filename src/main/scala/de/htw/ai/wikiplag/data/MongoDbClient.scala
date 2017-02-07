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
		val entry = inverseIndexCollection.findOneByID(token)

		entry match {
			case Some(x) => x.asInstanceOf[BasicDBObject].getAs[List[(Long, List[Int])]]("doc_list").get
			case None => List.empty[(Long, List[Int])]
		}
	}

	def getInvIndex(tokens: Set[String]): Map[String, List[(Long, List[Int])]] = {
		inverseIndexCollection.find("_id" $in tokens)
				.toList
				.map(MongoDbClient.parse2InvIndexEntry)
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
				.map(MongoDbClient.parse2InvIndexEntry)
				.foldLeft[RDD[(String, List[(Long, List[Int])])]](emptyRDD) {
			(rdd, x) => {
				val tmpRDD = sc.parallelize(Array(x))
				rdd.union(tmpRDD)
			}
		}
	}

	def getDocument(doc_id: Long): Document = {
		val documentEntry = documentsCollection.findOneByID(String.valueOf(doc_id))
		documentEntry match {
			case Some(x) => MongoDbClient.parse2Document(x.asInstanceOf[BasicDBObject])
			case None => null
		}
	}

	def getDocuments(list: Set[Long]): List[Document] = {
		documentsCollection
				.find("_id" $in list)
				.toList
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
		documentsCollection.insert(MongoDbClient.parseFromDocument(document))
	}

	def insertDocument(documents: Set[Document]): Unit = {
		val entries = documents.map(MongoDbClient.parseFromDocument)
		entries.foreach(x => documentsCollection.insert(x))
	}

	def insertInverseIndex(inverseIndex: Map[String, List[(Long, List[Int])]]): Unit = {
		val entries = inverseIndex.map(x => MongoDbClient.parseFromInverseIndexEntry(x._1, x._2))
		entries.foreach(x => inverseIndexCollection.insert(x))
	}

}

object MongoDbClient {

	protected final val WIKI_DATABASE = "wikiplag"
	protected final val COLLECTION_DOCUMENTS = "documents"
	protected final val COLLECTION_INVERSE_INDEX = "inv_idx"

	protected var serverAddress: ServerAddress = _
	protected var credentials: List[MongoCredential] = _

	private def parseFromDocument = (d: Document) => {
		MongoDBObject(
			("_id", d.id),
			("title", d.title),
			("text", d.text),
			("viewindex", d.viewIndex)
		)
	}

	private def parseFromInverseIndexEntry = (token: String, documents: List[(Long, List[Int])]) => {
		MongoDBObject(
			("_id", token),
			("doc_list", documents)
		)
	}

	private def parse2Document = (x: BasicDBObject) => {
		Document(
			id = x.getLong("_id", Long.MinValue),
			text = x.getString("text", ""),
			title = x.getString("title", ""),
			viewIndex = x.getAsOrElse[List[(Int, Int, Int)]]("viewindex", List.empty[(Int, Int, Int)])
		)
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

	def open(): MongoClient = {
		MongoClient(serverAddress, credentials)
	}

	def apply(sc: SparkContext, host: String, port: Integer, dbName: String, user: String, password: String): MongoDbClient = {
		serverAddress = new ServerAddress(host, port)
		credentials = List(MongoCredential.createCredential(user, dbName, password.toCharArray))

		val uriBuilder = new StringBuilder("mongodb://")
				.append(host)
				.append(":")
				.append(Integer.toString(port))
				.append("/")
				.append(dbName)
				.append('.')

		val authUriBuilder = new StringBuilder("mongodb://")
				.append(user)
				.append(":")
				.append(password)
				.append("@")
				.append(host)
				.append(":")
				.append(port)
				.append('/')
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
