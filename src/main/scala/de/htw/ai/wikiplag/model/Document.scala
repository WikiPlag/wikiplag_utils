package de.htw.ai.wikiplag.model

/**
  * Created by chris on 13.11.2016.
  */
case class Document(id: Long = Long.MinValue,
                    title: String = "",
                    text: String = "",
                    viewIndex: List[(Int, Int, Int)] = List.empty[(Int, Int, Int)]) extends Serializable
