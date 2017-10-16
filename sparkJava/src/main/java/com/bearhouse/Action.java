package com.bearhouse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by md186047 on 4/10/17.
 *
 *
 * This class is to show main actions in Spark
 */
public class Action {


    // initialize Spark
    SparkConf conf = new SparkConf().setAppName("wordcount");
    JavaSparkContext sc = new JavaSparkContext(conf);

    //load input data
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 6, 7, 8));

    //***************************************************************************
    // Fold() -- return the same data type. Take a "zero value" to begin with
    // Reduce() -- return the same data type
    //***************************************************************************

    Integer sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer x, Integer y) throws Exception {
            return x + y;
        }
    });

    //***************************************************************************
    // Aggregate() -- apply 2 functions to return different data type
    //***************************************************************************


    // return sum() and count()

    public class AvgCount implements Serializable {
        public int total;
        public int num;

        public AvgCount(int total, int num){
            this.total = total;
            this.num = num;
        }

        public double avg(){
            return total / (double) num;
        }
    }

    Function2<AvgCount, Integer, AvgCount> addAndCount =
        new Function2<AvgCount, Integer, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, Integer x) throws Exception {
                a.total += x;
                a.num += 1;
                return a; // return a AvgCount
            }
        };

    Function2<AvgCount, AvgCount, AvgCount> combine =
        new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, AvgCount b) throws Exception {
                a.total += b.total;
                a.num += b.num;
                return a;
            }
        };

    AvgCount initial = new AvgCount(0, 0 ); // initialized value
    AvgCount result = rdd.aggregate(initial, addAndCount, combine);
    double avgResult = result.avg();












}
