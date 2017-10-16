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
import org.apache.spark.rdd.RDD;

/**
 * Created by md186047 on 26/9/17.
 */
public class PassFunctions {


    public static void main(String s[]) {


        SparkConf conf = new SparkConf().setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //*********************************************************************
        // Function 1 -- java function passing with anonymous inner class
        //*********************************************************************

        JavaRDD<String> inputRDD = sc.textFile("log.txt");
        JavaRDD<String> errorRDD = inputRDD.filter(
            new Function<String, Boolean>() {
                public Boolean call(String x) {
                    return x.contains("error");
                }
            }
        );


        //*********************************************************************
        // Function 2 -- Java function passing with named class
        //*********************************************************************
        JavaRDD<String> errros = inputRDD.filter(new Contains("error"));


        //*********************************************************************
        // Function 3 -- Java 8 Lamda function
        //*********************************************************************
        JavaRDD<String> errors = inputRDD.filter(a -> a.contains("error"));
    }



    private static class Contains implements Function<String, Boolean> {

        private String query;
        public Contains(String query){ this.query = query;}
        public Boolean call(String s) throws Exception {
            return s.contains(query);
        }
    }






}


