package com.chimpler.sparknaivebayesreuters

import java.io.{FilenameFilter, File}

object NaiveBayesExample extends App {
  //    if (args.length == 0) {
  //      println("usage: naive <directory>")
  //      sys.exit()
  //    }
  //
  //    listFiles(args.head)
  listFiles("reuters")

  def listFiles(directory: String) = {
    val inputFiles = new File(directory).list(new FilenameFilter {
      override def accept(dir: File, name: String) = name.endsWith(".sgm")
    })

    val fullFileNames = inputFiles.map(directory + "/" + _)
    val docs = ReutersParser.parseAll(fullFileNames)


    println(docs)
  }
}
