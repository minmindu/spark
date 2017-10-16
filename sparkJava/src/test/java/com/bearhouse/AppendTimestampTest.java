package com.bearhouse;


import com.holdenkarau.spark.testing.JavaRDDComparisons;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by md186047 on 19/9/17.
 */
public class AppendTimestampTest  implements Serializable {

    private JavaSparkContext sc = new JavaSparkContext();

    @Test
    public void extractTimestampFromFileName(){

        String fileName = "testfile-01012017-000000.csv";
        String tsReg = "\\d{8}-\\d{6}";
        String tsFormat = "ddMMyyyy-HHmmss";

        String expectedResult = "2017-01-01 00:00:00";

        Assert.assertEquals(expectedResult, AppendTimestamp.parseTimestamp(fileName, tsReg, tsFormat));

    }

    @Test
    public void appendTimestampToLine() {
        String line1 = "the,quick,brown,fox";
        List<String> input = Arrays.asList(line1);

        JavaRDD inputRDD = sc.parallelize(input);
        String delimiter = ",";
        String timestamp = "2017-01-01 00:00:00";

        JavaRDD outputRDD = AppendTimestamp.addSourceTs(inputRDD, delimiter, timestamp);


        String expectedLine1 = "the,quick,brown,fox,2017-01-01 00:00:00";
        List<String> expect = Arrays.asList(expectedLine1);
        JavaRDD expectedRDD = sc.parallelize(input);

        JavaRDDComparisons.assertRDDEquals(expectedRDD, outputRDD);
    }

}
