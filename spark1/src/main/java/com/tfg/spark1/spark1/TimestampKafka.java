/**
 * @author  Francisco Robles Martin
 * @date	June 2017
 */

package com.tfg.spark1.spark1;

import java.util.Map;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;


public final class TimestampKafka {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: TimestampKafka <topics> <batch-interval>");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setMaster("spark://192.168.0.155:7077").setAppName("TimestampKafka");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(Integer.valueOf(args[1])));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "192.168.0.155:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spark");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList(args[0].split(","));

		final JavaInputDStream<ConsumerRecord<String, String>> messages =
				KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
						);

		JavaDStream<String> lines = messages.map(new MapperKafka()).
				persist(StorageLevel.MEMORY_ONLY()).repartition(4);

		JavaDStream<String> newLine = lines.map(new TimestampAdder());

		newLine.foreachRDD(new NetworkPublisher());

		jssc.start();
		jssc.awaitTermination();

	}

	//Functions used in the program implementations:

	public static class MapperKafka implements Function<ConsumerRecord<String, String>, String> {
		private static final long serialVersionUID = 1L;

		public String call(ConsumerRecord<String, String> record) {
			return record.value().toString();
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

	public static class NetworkPublisher implements VoidFunction<JavaRDD<String>> {
		private static final long serialVersionUID = 1L;

		public void call(JavaRDD<String> rdd) throws Exception {
			rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
				private static final long serialVersionUID = 1L;

				public void call(Iterator<String> partitionOfRecords) throws Exception {
					Socket mySocket = new Socket("192.168.0.155", 9998);
					final PrintWriter out = new PrintWriter(mySocket.getOutputStream(), true);
					while(partitionOfRecords.hasNext()) {
						out.println(partitionOfRecords.next());
					}
					mySocket.close();
				}
			});
		}
	};
}