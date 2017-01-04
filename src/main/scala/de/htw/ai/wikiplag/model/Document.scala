package de.htw.ai.wikiplag.model

import com.mongodb.DBObject

/**
  * Created by chris on 13.11.2016.
  */
class Document(val id: Long = Long.MinValue,
               val title: String = "",
               val text: String = "",
               val viewIndex:List[(Int,Int,Int)] = List.empty[(Int,Int,Int)]) extends Serializable {

//  val id: Long = lId
//  val title: String = sTitle
//  val text: String = sText
//  val viewIndex:List[(Int, Int, Int)] = viewInd

}
