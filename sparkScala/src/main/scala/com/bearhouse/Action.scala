package com.bearhouse

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by md186047 on 4/10/17.
  */
object Action {

  // init Spark context

  val conf = new SparkConf().setAppName("Action")
  val sc = new SparkContext(conf)

  val rdd = sc.parallelize(List(1, 2, 3, 5, 6, 9))

  //***************************************************************************
  // Fold() -- return the same data type. Take a "zero value" to begin with
  // Reduce() -- return the same data type
  //***************************************************************************
  val sum = rdd.reduce((x, y) => x + y)


  //***************************************************************************
  // Aggregate() -- apply 2 functions to return different data type
  //***************************************************************************

  // return sum and count
  // input is a pair
  val result = rdd.aggregate((0, 0))(
    (acc, value) => (acc._1 + value, acc._2 + 1), // function 1 for add(._1) and count (._2)
    (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)  // function 2 -- combine 2 element with the same type
  )

  // then get the avg
  val avg = result._1 / result._2.toDouble

}
