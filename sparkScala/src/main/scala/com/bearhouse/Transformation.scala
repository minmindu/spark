package com.bearhouse

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
/**
  * Created by md186047 on 21/9/17.
  */
object Transformation {

  def countWords(): Unit ={

    //load input data
    val inputFile = "/tmp/test.txt"
    val outputFile = "/tmp/test_output.txt"

    // init Spark
    val conf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(conf)

    // load input data
    val input = sc.textFile(inputFile)

    // split into words
    val words = input.flatMap(line => line.split(" "))

    // Transform into pairs and count
    val counts = words.map(words => (words, 1))
                      .reduceByKey{case (x, y) => x + y}

    // save the word count to a output file
    counts.saveAsTextFile(outputFile)


    //*********************************************************************
    // Filter
    //*********************************************************************

    val inputRDD = sc.textFile("log.txt")
    val errorRDD = inputRDD.filter(line => line.contains("error"))

    //*********************************************************************
    // Union
    //*********************************************************************
    val waringRDD = inputRDD.filter(line => line.contains("warning"))
    val badLinesRDD = errorRDD.union(waringRDD)


    //*********************************************************************
    // Action
    //*********************************************************************
    println("Input had " + badLinesRDD.count() + " concerning lines")
    println("Here is 10 examples : ")
    badLinesRDD.take(10).foreach(println)


  }

}
