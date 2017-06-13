/**
 * @author  	Francisco Robles Martin
 * @date		June 2017
 * @project 	Computer Science bachelor's final project:
 * 				Comparison between Spark Streaming and Flink
 * @university	Technical University of Madrid
 */

package kafka;

import java.util.Map;
import java.util.Properties;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
		if (args.length != 2) {
			System.err.println("Usage: TimestampKafka <topics> <batch-interval (ms)>");
			System.exit(1);
		}

		
		//SPARK CONFIGURATION
		SparkConf sparkConf = new SparkConf().setAppName("TimestampKafka");
		JavaStreamingContext jStreamingContext = new JavaStreamingContext(sparkConf, 
				Durations.milliseconds(Integer.valueOf(args[1])));
		
		
		//KAFKA CONSUMER CONFIGURATION
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "192.168.0.155:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spark");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		Collection<String> topics = Arrays.asList(args[0].split(","));

		final JavaInputDStream<ConsumerRecord<String, String>> message =
				KafkaUtils.createDirectStream(jStreamingContext, LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		
		//MAIN PROGRAM 
		JavaDStream<String> line = message.map(new MapperKafka()).
				persist(StorageLevel.MEMORY_ONLY()).repartition(4);

		JavaDStream<String> lineTS = line.map(new TimestampAdder());

		lineTS.foreachRDD(new KafkaPublisher());
		
		jStreamingContext.start();
		jStreamingContext.awaitTermination();

	}

	
	//FUNCTIONS used in the program implementations:

	public static class MapperKafka implements Function<ConsumerRecord<String, String>, String> {
		private static final long serialVersionUID = 1L;

		public String call(ConsumerRecord<String, String> record) throws Exception {
			return record.value().toString();
		}
	};

	public static class TimestampAdder implements Function<String, String> {
		private static final long serialVersionUID = 1L;

		public String call(String line) {
			Long currentTime = System.currentTimeMillis();
			String totalTime = String.valueOf(currentTime - Long.valueOf(line.split(" ")[1]));
			String newLine = line.concat(" " + String.valueOf(currentTime) + " " + totalTime);

			return newLine;
		}
	};

	public static class KafkaPublisher implements VoidFunction<JavaRDD<String>> {
		private static final long serialVersionUID = 1L;

		public void call(JavaRDD<String> rdd) throws Exception {
			
			//KAFKA PRODUCER
			Properties props = new Properties();
			props.put("bootstrap.servers", "192.168.0.155:9092");
			props.put("acks", "0");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 0);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			
			rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
				private static final long serialVersionUID = 1L;

				public void call(Iterator<String> partitionOfRecords) throws Exception {
					Producer<String, String> producer = new KafkaProducer<>(props);
					while(partitionOfRecords.hasNext()) {
						producer.send(new ProducerRecord<String, String>("testRes", partitionOfRecords.next()));
					}
					producer.close();
				}
			});
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