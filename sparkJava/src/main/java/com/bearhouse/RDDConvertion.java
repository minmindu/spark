package com.bearhouse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

import java.util.Arrays;

/**
 * Created by md186047 on 4/10/17.
 */
public class RDDConvertion {

    // Since Java is strict with data type, convertion between RDD is required whereas Scala handles it automatically
    // To use mapToDouble() function, etc.

    SparkConf conf = new SparkConf().setAppName("RDDConvertion");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 3, 2, 7, 6));

    // convert intRDD -> DoubleRDD

    JavaDoubleRDD result = rdd.mapToDouble(
        new DoubleFunction<Integer>() {
            @Override
            public double call(Integer x) throws Exception {
                return (double) x * x;
            }
        }
    );

}
