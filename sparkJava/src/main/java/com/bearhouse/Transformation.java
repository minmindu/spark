package com.bearhouse;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;

import scala.Tuple2;

/**
 * Created by md186047 on 21/9/17.
 */
public class Transformation {

    public static void wordCount(){

        //load input data
        String inputFile = "/tmp/test.txt";
        String outputFile = "/tmp/test_output.txt";

        // initialize Spark
        SparkConf conf = new SparkConf().setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //load input data
        JavaRDD<String> input = sc.textFile(inputFile);

        // split into words
        JavaRDD<String> words = input.flatMap(
            // the flatmap function in Java
            new FlatMapFunction<String, String>() {
                // s = input (a string)
                public Iterable<String> call(String s) throws Exception {
                    return Arrays.asList(s.split(" "));
                }
            }
        );

        // Transform into pairs and count
        JavaPairRDD<String, Integer> counts = words.mapToPair(
            new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String x){
                   return new Tuple2<String, Integer>(x, 1);
                }
            }
        ).reduceByKey(
            // reduce by key function:
            new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer x, Integer y) throws Exception {
                    return x +y;
                }
            }
        );

        // save the word count to the text file
        counts.saveAsTextFile(outputFile);


        //*********************************************************************
        // Filter
        //*********************************************************************
        JavaRDD<String> inputRDD = sc.textFile("log.txt");
        JavaRDD<String> errorRDD = inputRDD.filter(
            new Function<String, Boolean>() {
                public Boolean call(String x ){
                    return x.contains("error");
                }
            }
        );

        //*********************************************************************
        // Action
        //*********************************************************************

        // scala code
//        println("Input had " + badLinesRDD.count() + " concerning lines")
//        println("Here is 10 examples : ")
//        badLinesRDD.take(10).foreach(println)

        System.out.println("Input had " + errorRDD.count() + " concerning lines");
        System.out.println(" Here is 10 examples: ");
        for (String line: errorRDD.take(10)){
            System.out.println(line);
        }



    }

}
