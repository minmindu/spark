package com.bearhouse

import org.apache.spark.rdd.RDD

/**
  * Created by md186047 on 26/9/17.
  */
class SearchFunctions (val query: String){

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchesFunctionReference(rdd: RDD[String]) : RDD[Boolean] = {
    rdd.map(isMatch)
  }

  def getMatchesFieldReference(rdd: RDD[String]): RDD[String] = {
    rdd.map(x => x.split(query))
  }

  def getMatchesNOReference(rdd: RDD[String]) : RDD[String] = {
    val query_ = this.query
    rdd.map(x => x.split(query_))
  }

}
