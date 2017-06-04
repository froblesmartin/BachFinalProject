package com.tfg.spark1.spark1;

import java.util.Map;
import java.util.HashMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import org.apache.spark.streaming.kafka.*;

public final class TimestampKafka {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: TimestampKafka <topics> <numThreads> <batch-interval>");
			System.exit(1);
		}
	
		SparkConf conf = new SparkConf().setMaster("spark://192.168.0.155:7077").setAppName("TimestampKafka");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(Integer.valueOf(args[2])));
	
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		int numThreads = Integer.parseInt(args[1]);
		topicMap.put(args[0], numThreads);
		
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, "192.168.0.155:2181", "grupo-spark", topicMap); //Map<"test", 2>
		
		JavaDStream<String> lines = messages.map(new MapperKafka());
		
		JavaDStream<String> newLine = lines.map(new TimestampAdder());
		
		lines.print();
		newLine.print();

		jssc.start();
		jssc.awaitTermination();

	}
	
	//Functions used in the program implementations:
	
	public static class MapperKafka implements Function<Tuple2<String, String>, String> {
		private static final long serialVersionUID = 1L;

		public String call (Tuple2<String, String> tuple2) {
			return tuple2._2();
		}
	};
	
	public static class TimestampAdder implements Function<String, String> {
		private static final long serialVersionUID = 1L;

		public String call(String line) {
			String[] tuple = line.split(" ");
			String totalTime = String.valueOf(System.currentTimeMillis() - Long.valueOf(tuple[1]));
			String newLine = line.concat(" " + String.valueOf(System.currentTimeMillis()) + " " + totalTime);

			return newLine;
		}
	};
}