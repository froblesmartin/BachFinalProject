package com.tfg.spark1.spark1;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
//import java.util.regex.Pattern;
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache. spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

@SuppressWarnings("unused")
public final class WordCounter {

	public static void main(String[] args) throws Exception {
		/*if (args.length < 2) {
			System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
			System.exit(1);
		}*/
	

		SparkConf conf = new SparkConf().setMaster("spark://192.168.0.155:7077").setAppName("NetworkWordCount");
		@SuppressWarnings("resource")
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));

		JavaDStream<String> lines = jssc.textFileStream("file:///home/fran/nfs/nfs/in/");
		
		JavaDStream<Long> words = lines.count();
		
/*		JavaDStream<String> words = lines.flatMap(
				new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 1L;

					public Iterator<String> call(String x) {
						return Arrays.asList(x.split(" ")).iterator();
					}
				});

		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s,1);
					}
				});*/

/*		JavaDStream<String> palabrasConA = words.filter(
				new Function<String,Boolean>() {
					public Boolean call(String s) {
						return s.contains("a");
					}
				});
	
		
		JavaDStream<String> palabrasConB = words.filter(
				new Function<String,Boolean>() {
					public Boolean call(String s) {
						return s.contains("b");
					}
				});
		
		JavaDStream<String> palApalB = palabrasConA.union(palabrasConB);*/

/*		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});*/
		
/*		JavaDStream<Long> totalWords = pairs.count();
				
		totalWords.print();*/
		words.print();
//		palApalB.print();

		jssc.start();
		jssc.awaitTermination();

	}
}