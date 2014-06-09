package com.chimpler.sparknaivebayesreuters

import java.io.{FilenameFilter, File}
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable
import jline.ConsoleReader
import java.util

object NaiveBayesExample extends App {

  val sc = new SparkContext("local", "naivebayes")

  //    if (args.length == 0) {
  //      println("usage: naive <directory>")
  //      sys.exit()
  //    }
  //
  //    listFiles(args.head)
  val naiveBayesAndDictionaries = createNaiveBayesModel("reuters")
  console(naiveBayesAndDictionaries)

  /**
   * REPL loop to enter different urls
   */
  def console(naiveBayesAndDictionaries: NaiveBayesAndDictionaries) = {
    val consoleReader = new ConsoleReader()
    while ( {
      consoleReader.readLine("url> ") match {
        case s if s == "q" => true
        case url: String =>
          predict(naiveBayesAndDictionaries, url)
          false
        case _ => false
      }
    }) {}

    sc.stop()
  }

  def predict(naiveBayesAndDictionaries: NaiveBayesAndDictionaries, url: String) = {
//    Tokenizer.tokenize(content)
    val termsArray = new Array[Double](naiveBayesAndDictionaries.numTerms)
    util.Arrays.fill(termsArray, 0D)

    // extract content from web page

    // fill up array from dictionary

    naiveBayesAndDictionaries.model.predict(Vectors.dense(termsArray))
    // convert terms to vector if they exist

    // convert label from double
  }

  /**
   *
   * @param directory
   * @return
   */
  def createNaiveBayesModel(directory: String) = {
    val inputFiles = new File(directory).list(new FilenameFilter {
      override def accept(dir: File, name: String) = name.endsWith(".sgm")
    })

    val fullFileNames = inputFiles.map(directory + "/" + _)
    val docs = ReutersParser.parseAll(fullFileNames)
    val termDocs = Tokenizer.tokenizeAll(docs)

    // put collection in Spark
    val termDocsRdd = sc.parallelize[TermDoc](termDocs.toSeq)

    val numDocs = termDocs.size

    // create dictionary term => id
    // and id => term
    val terms = termDocsRdd.flatMap(_.terms).distinct().collect()
    val numTerms = terms.size
    val termsToIndex = terms.zipWithIndex.map {
      case (term, index) => term -> index
    }.toMap
    val indexToTerm = terms.zipWithIndex.map {
      case (term, index) => index -> term
    }.toMap

    val labels = termDocsRdd.flatMap(_.labels).distinct().collect()
    val labelsToIndex = labels.zipWithIndex.map {
      case (label, index) => label -> index
    }.toMap
    val indexToLabel = labels.zipWithIndex.map {
      case (label, index) => index -> label
    }.toMap

    // compute TFIDF and generate vectors
    // for IDF
    val numDocsPerTerm = (termDocsRdd.flatMap(termDoc => termDoc.terms.map((termDoc.doc, _))).distinct().groupBy(_._2) map {
      // too bad mapValues is not implemented
      case (term, docs) => term -> docs.size
    }).collect.toMap

    val tfidfs = termDocsRdd flatMap {
      termDoc =>
        val tfIdfsArray = new Array[Double](numTerms)
        util.Arrays.fill(tfIdfsArray, 0)

        termDoc.terms.groupBy(identity).foreach {
          case (term, instances) =>
            tfIdfsArray(termsToIndex(term)) = instances.size * math.log10(numDocs / numDocsPerTerm(term))
        }

        termDoc.labels.map {
          label =>
            val labelId = labelsToIndex(label).toDouble
            LabeledPoint(labelId, Vectors.dense(tfIdfsArray))
        }
    }

    val model = NaiveBayes.train(tfidfs)
    NaiveBayesAndDictionaries(model, numTerms, termsToIndex, indexToTerm, labelsToIndex, indexToLabel)
  }
}

case class NaiveBayesAndDictionaries(model: NaiveBayesModel,
                                     numTerms: Int,
                                     termsToIndex: Map[String, Int],
                                     indexToTerms: Map[Int, String],
                                     labelToIndex: Map[String, Int],
                                     indexToLabel: Map[Int, String])
