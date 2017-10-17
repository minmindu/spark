package com.bearhouse

import java.text.SimpleDateFormat
import java.util.logging.SimpleFormatter

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext

import scala.util.matching.Regex

/**
  * Created by md186047 on 20/9/17.
  */
object AppendTimestamp {




  def main(args: Array[String]): Unit ={

    val log = LogManager.getRootLogger

    log.info("Running Spark AppendTimestamp with the following command line args (comma separated)")

    if(args.length < 8 && args.length > 9){
      System.out.println("Please provide 8 parameters");
    }

    try{
      val categoryName = args(0)
      val feedName = args(1)
      val partitionTs = args(2)
      val sourceRoot = args(3)
      val ingestRoot = args(4)
      val filename = args(5)
      val sourceTimestampPattern = args(6).r // .r for regular expression
      val sourceDateFormat = args(7)
      val delimiter = if (args.length ==8 )  "," else args(8)

      val outputDateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      // TOGET : how to combine a string
      val partitionPath = s"$categoryName/$feedName/$partitionTs"

      // init Spark Context
      val sc = new SparkContext()

      // timestamp matching
      val outputTimestamp = parseTimestamp(filename, sourceTimestampPattern, sourceDateFormat, outputDateTimeFormatter)

      if(outputTimestamp.nonEmpty) {

        // Add timestamp to data
        val sourceDataRDD = sc.textFile(sourceRoot + partitionPath)
        val ingestDateRDD = sourceDataRDD.map(line => appendTs(line, delimiter, outputTimestamp))

        // save output to a file
        ingestDateRDD.saveAsTextFile(ingestRoot + partitionPath)
      }
      // Fail if not match
      else System.exit(1)
    }
    catch {
      case e: Exception => {
        System.out.println(e)
        System.exit(1)
      }
    }
  }

  // TOGET: a function returns Option[dataType] rather than a dataType straightaway in order to check NULL value
  def parseTimestamp(filename: String, tsRegex: Regex, inputDateFormat: String, outputDateTimeFormatter: SimpleDateFormat): Option[String] = {

    // to get  timestamp string from file name
    val inputTsOption = tsRegex.findFirstMatchIn(filename)

    if(inputTsOption.nonEmpty) {
      val inputTsFormatter = new SimpleDateFormat(inputDateFormat)
      val inputTs = inputTsFormatter.parse(inputTsOption.get.toString())

      val outputFormattedTimestamp = outputDateTimeFormatter.format(inputTs)

      // TOGET: no return statement, just put an expression
      Some(outputFormattedTimestamp)
    }
    else None

  }


  def appendTs(line: String, delimiter: String, outputTimestamp: Option[String]): String = {
    line + delimiter + outputTimestamp
  }

}
