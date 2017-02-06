package de.htw.ai.wikiplag.data

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


/**
  * Created by chris on 06.11.2016.
  */
@RunWith(classOf[JUnitRunner])
class InverseIndexBuilderTest extends FunSuite {

	trait TestObject {
		val testObject = InverseIndexBuilderImpl
		testObject.loadStopWords()
	}

	test("testEmptyList") {


		new TestObject {
			val map_e1 = testObject.buildIndexKeys("")

			assert(map_e1.isEmpty)

		}
	}

	test("stopword removal") {
		val input = List("abc", "def", "ist", "einen", "ghi")
		val expected = List("abc", "def", "ghi")
		new TestObject {
			val out = testObject.normalize(input)
			assert(out == expected)
		}
	}

	test("testBuildInverseIndexEntry(doc_id, pageWordsAsList)") {

		val doc_id = 13
		val temsInDocument1 = List("Ä", "Ü", "Ö", "Ελλάδα", "Elláda", "Ä", "Ü", "Ö", "Ελλάδα", "Elláda")

		new TestObject {
			val map_e1 = testObject.buildInverseIndexEntry(doc_id, temsInDocument1)

			assert(map_e1.size == 5)
			assert(map_e1.get("Ä").get._1 == doc_id)
			assert(map_e1.get("Ä").get._2.size == 2)
			assert(map_e1.get("Ä").get._2(1) == 5)

		}
	}

	test("testMergeInverseIndexEntries(entries, pageWordsAsList)") {

		val temsInDocument1 = List("Ä", "Ü", "Ö", "Ελλάδα", "Elláda", "Ä", "Ü", "Ö", "Ελλάδα", "Elláda")
		val temsInDocument2 = List("Ä", "Ü", "Ö", "Ελλάδα", "Elláda", "Ä", "Ü", "Ö", "Ελλάδα", "Elláda")
		val temsInDocument3 = List("Äplle", "Apfel", "_apfel", "Ö", "Elláda", "Ä", "Ü", "Ö", "Ελλάδα", "Elláda")
		val temsInDocument4 = List("Alan", "Smithee", "steht", "als", "Pseudonym")

		new TestObject {
			val map_e1 = testObject.buildInverseIndexEntry(12, temsInDocument1)
			val map_e2 = testObject.buildInverseIndexEntry(13, temsInDocument2)
			val map_e3 = testObject.buildInverseIndexEntry(14, temsInDocument3)
			val map_e4 = testObject.buildInverseIndexEntry(15, temsInDocument4)

			val mapEntries = List(map_e1, map_e2, map_e3, map_e4)

			val inverseIndex = testObject.mergeInverseIndexEntries(mapEntries)

			assert(map_e1.size == 5)

		}
	}

	test("testKeyBuilder") {
		val docText = "1997 kam die Parodie die kam Parodie An Alan Smithee Film: Burn Hollywood"

		new TestObject {
			val resultList = testObject.buildIndexKeys(docText)
			val resultSet = testObject.buildIndexKeySet(docText)

			assert(resultList.size == 10)
			assert(resultList.head.equals("diekam"))
			assert(resultList(1).equals("Parodiedie"))
			assert(resultList(2).equals("Parodiedie"))

			assert(resultSet.size == 9)
			assert(resultSet.contains("diekam"))
			assert(resultSet.contains("Parodiedie"))
			assert(resultSet.contains("Parodiekam"))

		}
	}

}
