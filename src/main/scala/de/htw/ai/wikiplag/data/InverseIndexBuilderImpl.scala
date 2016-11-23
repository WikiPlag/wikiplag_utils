package de.htw.ai.wikiplag.data

import de.htw.ai.wikiplag.forwardreferencetable.ForwardReferenceTable

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
    * @param pageWordsAsList Eine Liste, deren Elemente die Woerter der Page enthalten.
    * @return Eine Forward Reference Table.
    */
  def buildInverseIndexEntry(doc_id: Int,
                             pageWordsAsList: List[String]): Map[String, (Int, List[Int])] = {
    pageWordsAsList.foldLeft((Map.empty[String, (Int, List[Int])], 0)) {
      (entry, x) => {
        val docList = entry._1.getOrElse(x, (doc_id, List.empty[Int]))._2
        (entry._1.updated(x, (doc_id, docList:+ entry._2)), entry._2 + 1)
      }
    }._1
  }

  def mergeInverseIndexEntries(entries: List[Map[String, (Int, List[Int])]]): Map[String, List[(Int, List[Int])]] = {
    entries.foldLeft(Map.empty[String, List[(Int, List[Int])]]) {
      (map, entryMap) => {

        entryMap.foldLeft(map) {
          (map, x) => {
            val docList = map.getOrElse(x._1, List.empty[(Int, List[Int])])
            map.updated(x._1, docList:+ x._2)
          }
        }
      }
    }
  }


}
