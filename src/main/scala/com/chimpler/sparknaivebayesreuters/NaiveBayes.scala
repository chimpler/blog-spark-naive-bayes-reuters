package com.chimpler.sparknaivebayesreuters

import java.io.{FilenameFilter, File}
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable
import com.gravity.goose.{Goose, Configuration}
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
    println("Enter 'q' to quit")
    val consoleReader = new ConsoleReader()
    while ( {
      consoleReader.readLine("url> ") match {
        case s if s == "q" => false
        case url: String =>
          predict(naiveBayesAndDictionaries, url)
          true
        case _ => true
      }
    }) {}

    sc.stop()
  }

  def predict(naiveBayesAndDictionaries: NaiveBayesAndDictionaries, url: String) = {
    // extract content from web page
    val config = new Configuration
    config.setEnableImageFetching(false)
    val goose = new Goose(config)
    val content = goose.extractContent(url).cleanedArticleText
    val tokens = Tokenizer.tokenize(content)

    val tfIdfs = naiveBayesAndDictionaries.termDictionary.tfIdfs(tokens, naiveBayesAndDictionaries.idfs)
    val vector = naiveBayesAndDictionaries.termDictionary.vectorize(tfIdfs)
    println("---> " + vector)
    val labelId = naiveBayesAndDictionaries.model.predict(vector)

    // convert label from double
    println("Label: " + naiveBayesAndDictionaries.labelDictionary.valueOf(labelId.toInt))
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
    val termDict = new Dictionary(terms)

    val labels = termDocsRdd.flatMap(_.labels).distinct().collect()
    val labelDict = new Dictionary(labels)

    // compute TFIDF and generate vectors
    // for IDF
    val idfs = (termDocsRdd.flatMap(termDoc => termDoc.terms.map((termDoc.doc, _))).distinct().groupBy(_._2) map {
      // mapValues not implemented :-(
      case (termIndex, docs) => termIndex -> math.log(numDocs.toDouble / docs.size.toDouble)
    }).collect.toMap

    val tfidfs = termDocsRdd flatMap {
      termDoc =>
        val termPairs = termDict.tfIdfs(termDoc.terms, idfs)
        termDoc.labels.map {
          label =>
            val labelId = labelDict.indexOf(label).toDouble
            val vector = Vectors.sparse(termDict.count, termPairs)
            LabeledPoint(labelId, vector)
        }
    }

    println("TFIDFS: " + tfidfs.collect().head.features.toArray.mkString(","))
    println("LABELS: " + labels.mkString("[", ", ", "]"))
    val model = NaiveBayes.train(tfidfs)
    NaiveBayesAndDictionaries(model, termDict, labelDict, idfs)
  }
}

case class NaiveBayesAndDictionaries(model: NaiveBayesModel,
                                     termDictionary: Dictionary,
                                     labelDictionary: Dictionary,
                                     idfs: Map[String, Double])
