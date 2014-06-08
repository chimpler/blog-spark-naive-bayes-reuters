package com.chimpler.sparknaivebayesreuters

import scala.xml.pull.{EvText, EvElemEnd, EvElemStart, XMLEventReader}
import scala.io.Source
import scala.collection.mutable

object ReutersParser {
  def parseAll(xmlFiles: Iterable[String]) = xmlFiles flatMap parse

  def parse(xmlFile: String) = {
    val docs = mutable.ArrayBuffer.empty[ReutersDoc]
    val xml = new XMLEventReader(Source.fromFile(xmlFile, "latin1"))
    var currentDoc = ReutersDoc()
    var inTopics = false
    var inLabel = false
    var inBody = false
    for (event <- xml) {
      event match {
        case EvElemStart(_, "REUTERS", _, _) => currentDoc = ReutersDoc()

        case EvElemEnd(_, "REUTERS") =>
          if (currentDoc.labels.nonEmpty) {
            docs += currentDoc
          }

        case EvElemStart(_, "TOPICS", _, _) => inTopics = true

        case EvElemEnd(_, "TOPICS") => inTopics = false

        case EvElemStart(_, "D", _, _) => inLabel = true

        case EvElemEnd(_, "D") => inLabel = false

        case EvElemStart(_, "BODY", _, _) => inBody = true

        case EvElemEnd(_, "BODY") => inBody = false

        case EvText(text) =>
          if (text.trim.nonEmpty) {
            if (inTopics && inLabel) {
              currentDoc = currentDoc.copy(labels = currentDoc.labels + text)
            } else if (inBody) {
              currentDoc = currentDoc.copy(body = currentDoc.body + text.trim)
            }
          }

        case _ =>
      }
    }
    docs
  }
}

case class ReutersDoc(body: String = "", labels: Set[String] = Set.empty)