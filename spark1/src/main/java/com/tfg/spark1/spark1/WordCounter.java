package com.tfg.spark1.spark1;

import java.util.Arrays;
import java.util.Iterator;
//import java.util.regex.Pattern;

import org.apache.spark.*;
import org.apache.spark.api.java.StorageLevels;
import org.apache. spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

public final class WordCounter {
	
	public static void main(String[] args) throws Exception {
		/*if (args.length < 2) {
			System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
			System.exit(1);
		}*/



		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 
				9999,StorageLevels.MEMORY_AND_DISK_SER);

		JavaDStream<String> words = lines.flatMap(
				new FlatMapFunction<String, String>() {
					public Iterator<String> call(String x) {
						return Arrays.asList(x.split(" ")).iterator();
					}
				});

		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s,1);
					}
				});
		
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		wordCounts.print();

		jssc.start();
		jssc.awaitTermination();

	}
}