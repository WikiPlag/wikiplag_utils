package de.htw.ai.wikiplag.model

/**
  * Created by chris on 13.11.2016.
  */
abstract class Document {

  val id:Long
  val title:String
  val text:String
  val viewIndex:List[(Int, Int, Int)]

}
