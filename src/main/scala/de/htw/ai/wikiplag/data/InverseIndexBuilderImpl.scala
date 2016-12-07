package de.htw.ai.wikiplag.data

import de.htw.ai.wikiplag.forwardreferencetable.ForwardReferenceTable
import de.htw.ai.wikiplag.parser.WikiDumpParser

import scala.StringBuilder
import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Created by chris on 06.11.2016.
  */
object InverseIndexBuilderImpl {

  /**
    * Erzeugt eine ForwardReferenceTable nach dem Schema:
    * {
    * "hash("ngram_1")":  List[ ( page_id, List[ ngram_position_1, ngram_position_2, ngram_position_3 ] ) ],
    * "hash("ngram_2")":  List[ ( page_id, List[ ngram_position_1, ... ] ) ], ...
    * }
    *
    * Beispiel:
    *
    * Input:
    * pageId = Int(1)
    * pageWordsAsList = List[String]("kam", "die", "Parodie", "An", "Alan", "Smithee", "Film", "Burn", "Hollywood")
    * stepSize = Int(3)
    *
    * Output:
    * collection.mutable.Map[String, List[(Int, List[Int])]
    * {
    * "hash("kam die Parodie")": List[ ( 1, List[ 0 ] ) ],
    * "hash("die Parodie An")": List[ ( 1, List[ 1 ] ) ],
    * "hash("Parodie An Alan")": List[ ( 1, List[ 2 ] ) ], ...
    * }
    *
    * @param doc_id          Die Page-ID.
    * @param tokens Eine Liste, deren Elemente die Woerter der Page enthalten.
    * @return Eine Forward Reference Table.
    */
  def buildInverseIndexEntry(doc_id: Long,
                             tokens: List[String]): Map[String, (Long, List[Int])] = {
    tokens.foldLeft((Map.empty[String, (Long, List[Int])], 0)) {
      (entry, x) => {
        val docList = entry._1.getOrElse(x, (doc_id, List.empty[Int]))._2
        (entry._1.updated(x, (doc_id, docList:+ entry._2)), entry._2 + 1)
      }
    }._1
  }

  def mergeInverseIndexEntries(entries: List[Map[String, (Long, List[Int])]]): Map[String, List[(Long, List[Int])]] = {
    entries.foldLeft(Map.empty[String, List[(Long, List[Int])]]) {
      (map, entryMap) => {

        entryMap.foldLeft(map) {
          (map, x) => {
            val docList = map.getOrElse(x._1, List.empty[(Long, List[Int])])
            map.updated(x._1, docList:+ x._2)
          }
        }
      }
    }
  }

  def buildIndexKeySet(documentText : String) : Set[String] = {
    buildIndexKeys(documentText).toSet
  }

  def buildIndexKeys(documentText : String): List[String] ={
    val tokens = WikiDumpParser.extractPlainText(documentText)
    build2TokenKeys(tokens, List.empty[String])
  }

  @tailrec
  private def build2TokenKeys(uniqueTokens : List[String], agg : List[String]) : List[String]= {
    if (uniqueTokens.isEmpty)
      return agg

    val head = uniqueTokens.head
    val tail = uniqueTokens.tail

    if (tail == null || tail == List.empty[String])
      return agg

    val strings = List(head, tail.head).sortWith((e1, e2) => (e1 compareTo e2) < 0)

    build2TokenKeys(uniqueTokens.tail, agg :+ strings.mkString )
  }

}
