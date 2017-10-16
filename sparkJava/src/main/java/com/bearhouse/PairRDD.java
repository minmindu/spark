package com.bearhouse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;

import scala.Tuple2;

/**
 * Created by md186047 on 4/10/17.
 */
public class PairRDD {

    SparkConf conf = new SparkConf().setAppName("PairRDD");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello World", "Hello Minmin", "Good practice"));

    /**
     *  create a pair RDD using the first word as the key
     * expected result : key - Hello, value - Hello World
                         key - Hello, value - Hello Minmin
                         key - Good,  value - Good practice
     */

    PairFunction<String, String, String> keyData =
        new PairFunction<String, String, String>() {

            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0], s);
            }
        };
}


