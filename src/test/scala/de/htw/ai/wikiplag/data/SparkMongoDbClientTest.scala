package de.htw.ai.wikiplag.data


import java.io.FileInputStream
import java.util.Properties

import com.mongodb.ServerAddress
import com.mongodb.casbah.MongoCredential
import com.mongodb.casbah.Imports._
import org.apache.commons.cli.GnuParser
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Created by chris on 14.12.2016.
  */
class SparkMongoDbClientTest extends FunSuite with BeforeAndAfterAll {
  var mongoDbClient: MongoDbClient = _
  var conf:org.apache.spark.SparkConf=_
  var sc:SparkContext=_


  override protected def beforeAll() {

    val (host, port, dbName, user, password) =
      try {
        val prop = new Properties()
        prop.load(new FileInputStream("mongo.properties"))

        (
          prop.getProperty("mongo.host"),
          new Integer(prop.getProperty("mongo.port")),
          prop.getProperty("mongo.db"),
          prop.getProperty("mongo.user"),
          prop.getProperty("mongo.password")
          )
      } catch { case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
      }

    conf = new SparkConf().setMaster("local[4]").setAppName("MongoDbClientTest")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.storage.memoryFraction", "0.8")
    conf.set("spark.driver.memory", "2g")

//    val uri = "mongodb://" + host + ":" + port + "/wikiplag.documents"
//    val authUri = "mongodb://" + user + ":" + password + "@" + host + ":" + port + "/wikiplag"
//
//    conf.set("mongo.input.uri", uri)
//    conf.set("mongo.auth.uri", authUri)

    sc = new SparkContext(conf)

    mongoDbClient = MongoDbClient(sc, host, port, user, dbName, password)

    //    entityResolution = new EntityResolution(sc, dat1, dat2, stopwordsFile, goldStandardFile)
    //amazon_small= Utils.getData(dat1, sc).cache
    //google_small= Utils.getData(dat2, sc).cache
    //google_small.take(5)
    //goldStandard= Utils.getGoldStandard("Amazon_Google_perfectMapping.csv", sc).cache

  }

  test("testGetDocumentsRDD") {
    val ids = Set(603076L, 5493599L, 7817367L, 302981L, 8188349L, 4363290L, 4914911L, 8547748L, 6465788L, 7417081L, 8188350L, 7417082L, 33652L, 1698103L, 2277221L, 4169027L, 3604438L, 6243799L, 7817368L, 8855922L, 8188352L,
      8910130L, 1284907L, 6767096L, 2397761L, 8427917L, 6317089L, 224556L, 7243578L, 7689003L, 8018735L, 4709493L, 187902L, 8377699L, 4074338L, 487381L, 99746L, 8075522L, 6243800L, 187905L, 4074341L, 603077L, 6767097L, 3604446L,
      3513886L, 8910131L, 4169030L, 7243579L, 8427918L, 6317090L, 6243801L, 4074342L, 2397766L, 3604451L, 487382L, 2397768L, 7243582L, 2831742L, 487384L, 720706L, 4169035L, 157017L, 4525472L, 1438762L, 2925884L, 846769L, 8547750L,
      9365943L, 5648947L, 8315481L, 5206547L, 2633792L, 1438760L, 3772027L, 5828266L, 6158502L, 487385L, 302982L, 1959867L, 5493600L, 5648950L, 3772028L, 5828272L, 302983L, 5738818L, 2925887L, 1438764L, 3772029L, 2633796L, 3212281L,
      6906016L, 4608048L, 6465789L, 2925888L, 5493605L, 7417083L, 603078L, 3772030L, 439134L, 8188356L)

    val testDocumentsRDD = mongoDbClient.getDocumentsRDD(ids)

    assert (testDocumentsRDD != null)
//    assert(testDocumentsRDD.count() == ids.size)

    val testDocuments = testDocumentsRDD.collect().toList

    assert (testDocuments.size == ids.size)

    val testDoc = testDocuments.find(x => x.id == 187902).get
    assert (testDoc.title.equals("Bechuanaland"))


  }

  test("testGetInvIndexRDD") {
    val tokens= Set[String]("Rausch", "Schokoladen", "Wilhelm", "Rausch", "Sohn","Konditormeisters","Chocolatiers","eröffnete","Berlin","Rausch","Privat","Confiserie","Herstellung","Pralinen","Schokoladen","Honigkuchen")

    val testEntriesRDD = mongoDbClient.getInvIndexRDD(tokens)

    assert (testEntriesRDD != null)

    val inverseIndex = testEntriesRDD.collect().toMap

    assert (inverseIndex.size == tokens.size)

    val testEntry = inverseIndex.get("Rausch").get

    // find a valid document
    assert(testEntry.exists(x => x._1.equals(5712984L)))
  }

  override protected def afterAll() {
    if (sc!=null) {sc.stop; println("Spark stopped......")}
    else println("Cannot stop spark - reference lost!!!!")
  }
}
