blog-spark-naive-bayes-reuters
==============================

Simple example on how to use Naive Bayes on Spark using the popular Reuters 21578 dataset. More info on our blog: http://chimpler.wordpress.com/2014/06/11/classifiying-documents-using-naive-bayes-on-apache-spark-mllib/

Requirements
============

GravityLabs Goose (use our fork to be compatible with Scala 2.10):
    
    $ git clone http://github.com/Chimpler/goose
    $ mvn install
    
Setup
=====

    $ ./download_reuters.sh

Running classification
======================

    $ sbt run    
    
Examples
========

    http://www.coinflation.com/coins/1942-1945-Silver-War-Nickel-Value.html => gold
    http://www.businessweek.com/news/2014-06-10/china-using-dubai-style-fake-islands-to-reshape-south-china-sea => ship
    http://en.wikipedia.org/wiki/Soybean => grain
    http://en.wikipedia.org/wiki/Whole_wheat_bread => grain
    http://en.wiktionary.org/wiki/cow => livestock
