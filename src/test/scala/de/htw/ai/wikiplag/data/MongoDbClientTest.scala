package de.htw.ai.wikiplag.data

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by chris on 30.11.2016.
  */
@RunWith(classOf[JUnitRunner])
class MongoDbClientTest extends FunSuite with BeforeAndAfterAll {
	var mongoDbClient: MongoDbClient = _

	override protected def beforeAll(): Unit = {
		val config = ConfigFactory.load("mongo.properties")
		mongoDbClient = MongoDbClient(
			null,
			config.getString("mongo.host"),
			config.getInt("mongo.port"),
			config.getString("mongo.db"),
			config.getString("mongo.user"),
			config.getString("mongo.password")
		)
	}

	/*
	 * Tests
	 */

	test("testGetInvIndex_singleEntry") {
		val data = mongoDbClient.getInvIndex("denNamen")
		assert(data != null)
	}

	test("testGetInvIndex") {

	}

	test("testGetDocument") {
		val docId = 6465788L
		val id2 = 603076L
		val id3 = 9095622L
		val testDocument = mongoDbClient.getDocument(docId)

		assert(testDocument != null)
		assert(testDocument.id == docId)
	}

	test("testGetDocuments") {
		val ids = Set(6465788L, 603076L, 9095622L)
		val testDocuments = mongoDbClient.getDocuments(ids)

		assert(testDocuments.size == 3)
		assert(testDocuments.filter(x => x.id == 603076L).head.text.isEmpty)
		assert(testDocuments.filter(x => x.id == 6465788L).head.title.equals("USS Anchorage"))
	}

	test("testGet100DocumentsBuildInverseIndexAndStoreItToTheDatabase") {
		val ids = Set(603076L, 5493599L, 7817367L, 302981L, 8188349L, 4363290L, 4914911L, 8547748L, 6465788L, 7417081L, 8188350L, 7417082L, 33652L, 1698103L, 2277221L, 4169027L, 3604438L, 6243799L, 7817368L, 8855922L, 8188352L,
			8910130L, 1284907L, 6767096L, 2397761L, 8427917L, 6317089L, 224556L, 7243578L, 7689003L, 8018735L, 4709493L, 187902L, 8377699L, 4074338L, 487381L, 99746L, 8075522L, 6243800L, 187905L, 4074341L, 603077L, 6767097L, 3604446L,
			3513886L, 8910131L, 4169030L, 7243579L, 8427918L, 6317090L, 6243801L, 4074342L, 2397766L, 3604451L, 487382L, 2397768L, 7243582L, 2831742L, 487384L, 720706L, 4169035L, 157017L, 4525472L, 1438762L, 2925884L, 846769L, 8547750L,
			9365943L, 5648947L, 8315481L, 5206547L, 2633792L, 1438760L, 3772027L, 5828266L, 6158502L, 487385L, 302982L, 1959867L, 5493600L, 5648950L, 3772028L, 5828272L, 302983L, 5738818L, 2925887L, 1438764L, 3772029L, 2633796L, 3212281L,
			6906016L, 4608048L, 6465789L, 2925888L, 5493605L, 7417083L, 603078L, 3772030L, 439134L, 8188356L)

		val testDocuments = mongoDbClient.getDocuments(ids)

		assert(testDocuments.size == ids.size)

		val invIndexes = testDocuments.map(d => {
			val documentTokens = InverseIndexBuilderImpl.buildIndexKeys(d.text)

			InverseIndexBuilderImpl.buildInverseIndexEntry(d.id, documentTokens)
		})

		val finalInverseIndex = InverseIndexBuilderImpl.mergeInverseIndexEntries(invIndexes)

		mongoDbClient.insertInverseIndex(finalInverseIndex)
	}

	test("testGetDocumentsRDD") {

	}

	test("testDocumentsCollection") {

	}

	test("testInverseIndexCollection") {

	}

	test("testGetInvIndexRDD") {

	}

}
