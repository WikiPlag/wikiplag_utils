package de.htw.ai.wikiplag.model

import com.mongodb.DBObject

/**
  * Created by chris on 13.11.2016.
  */
class Document(lId: Long = Long.MinValue,
               sTitle: String = "",
               sText: String = "",
               viewInd:List[(Int,Int,Int)] = List.empty[(Int,Int,Int)]) {

  val id: Long = lId
  val title: String = sTitle
  val text: String = sText
  val viewIndex:List[(Int, Int, Int)] = viewInd

}
