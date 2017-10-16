package com.bearhouse;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.RegularExpression;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.util.matching.Regex;

/**
 * Created by md186047 on 18/9/17.
 */
public class AppendTimestamp {

    private static final Logger log = LoggerFactory.getLogger(AppendTimestamp.class);

    public static void main(String args[]){

        log.info("Running Spark AppendTimestamp ...");

        if(args.length < 8 || args.length > 9){
            System.out.println("No right number of arguments. Please provide 8 arguments");
            System.exit(1);
        }


            String categoryName = args[0];
            String feedName = args[1];
            String partitionTs = args[2];
            String sourceRoot = args[3];
            String ingestRoot = args[4];
            String fileName = args[5];
            String sourceTimestampPatten = args[6];
            String dateFormat = args[7];
            final String delimiter = (args[8] == null ) ? "," : args[8];

            final SimpleDateFormat outputDateTimesFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String partitionPath = categoryName + "/" + feedName +"/" + partitionTs + "/";

            // Initialize Spark Content
            JavaSparkContext sc = new JavaSparkContext();


            final String outputTimestamp = parseTimestamp(fileName, sourceTimestampPatten, dateFormat);

            if(!(outputTimestamp == null )){

                // add timestamp to data
                String path = sourceRoot +  partitionPath;
                JavaRDD sourceDataRDD = sc.textFile(path);
                JavaRDD<String> ingestDataRDD = addSourceTs(sourceDataRDD, delimiter, outputTimestamp)
;
                // save the output
                ingestDataRDD.saveAsObjectFile(path);
            } else{
                System.exit(1);
            }

    }

    public static String parseTimestamp(String fileName, String tsRegex, String tsFormat) {
        Pattern regex = Pattern.compile(tsRegex);
        Matcher regexMatcher = regex.matcher(fileName);

        if (regexMatcher.find()) {
            String inputTsString = regexMatcher.group();

            SimpleDateFormat inputTsFormatter = new SimpleDateFormat(tsFormat);
            try {
                Date inputTs = inputTsFormatter.parse(inputTsString);

                SimpleDateFormat outputTsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String outputFormattedTs = outputTsFormat.format(inputTs);
                return outputFormattedTs;

            } catch (ParseException e) {
                log.error(String.format("ERROR - Cannot convert timestamp {} based on format {}", inputTsString, tsFormat));
                e.printStackTrace();
            }
        }

        return null;
    }

    public static JavaRDD<String> addSourceTs(JavaRDD sourceDataRDD, final String delimiter, final String outputTimestamp){
        return sourceDataRDD.map(
            new Function<String, String>() {
                public String call(String line) throws Exception {
                    return line + delimiter + outputTimestamp;
                }

            }
        );
    }



}

























