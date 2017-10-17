package com.bearhouse

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by md186047 on 4/10/17.
  */
object PairRDD {


  val conf = new SparkConf().setAppName("PairRDD")
  val sc = new SparkContext(conf)

  val rdd = sc.parallelize(List("Hello World"
                                , "Hello Minmin"
                                , "Good practice"))

  /**
    * create a pair RDD using the first word as the key
    * expected result : key - Hello, value - Hello World
    * key - Hello, value - Hello Minmin
    * key - Good,  value - Good practice
    */

  val pairs = rdd.map(line => (line.split(" ")(0), line))

}
