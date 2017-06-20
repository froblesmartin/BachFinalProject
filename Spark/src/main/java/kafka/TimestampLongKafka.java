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

import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

public final class TimestampLongKafka {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: TimestampLongKafka <topics> <batch-interval (ms)> <checkpointing>");
			System.err.println("\t <checkpointing>: [0|1]");
			System.exit(1);
		}


		//SPARK CONFIGURATION
		SparkConf sparkConf = new SparkConf().setAppName("TimestampLongKafka");
		JavaStreamingContext jStreamingContext = new JavaStreamingContext(sparkConf, 
				Durations.milliseconds(Integer.valueOf(args[1])));

		if (Integer.valueOf(args[2]) == 1) { 
			jStreamingContext.checkpoint("file:///home/fran/nfs/nfs/checkpoints/spark");
		}

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
				persist(StorageLevel.MEMORY_ONLY());

		//Add 1 to each line
		JavaDStream<Tuple2<String, Integer>> line_Num = line.map(new NumberAdder());
		
		//Filter Odd numbers
		JavaDStream<Tuple2<String, Integer>> line_Num_Odd = line_Num.filter(new FilterOdd());
		JavaDStream<Tuple3<String, String, Integer>> line_Num_Odd_2 = line_Num_Odd.map(new OddAdder());
		
		//Filter Even numbers
		JavaDStream<Tuple2<String, Integer>> line_Num_Even = line_Num.filter(new FilterEven());
		JavaDStream<Tuple3<String, String, Integer>> line_Num_Even_2 = line_Num_Even.map(new EvenAdder());
		
		//Join Even and Odd
		JavaDStream<Tuple3<String, String, Integer>> line_Num_U = line_Num_Odd_2.union(line_Num_Even_2);
		
		//Change to JavaPairDStream
		JavaPairDStream<String, Tuple2<String, Integer>> line_Num_U_K = line_Num_U.mapToPair(new Keyer());
		
		//Tumbling windows every 2 seconds
		JavaPairDStream<String, Tuple2<String, Integer>> windowedLine_Num_U_K = line_Num_U_K
				.window(Durations.seconds(2), Durations.seconds(2));
		
		//Reduce to one line with the sum
		JavaDStream<Tuple2<String, Tuple2<String, Integer>>> wL_Num_U_Reduced = windowedLine_Num_U_K.reduceByKey(new Reducer())
				.toJavaDStream();
		
		//Calculate the average of the elements summed
		JavaDStream<String> wL_Average = wL_Num_U_Reduced.map(new AverageCalculator());
		
		//Add timestamp and calculate the difference with the average
		JavaDStream<String> averageTS = wL_Average.map(new TimestampAdder());
		

		//Send the result to Kafka
		averageTS.foreachRDD(new KafkaPublisher());

		jStreamingContext.start();
		jStreamingContext.awaitTermination();

	}


	//Functions used in the program implementation:

	public static class OddAdder implements Function<Tuple2<String, Integer>, Tuple3<String, String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple3<String, String, Integer> call(Tuple2<String, Integer> line) throws Exception {
			Tuple3<String, String, Integer> newLine = new Tuple3<String, String, Integer>(line._1, "odd", line._2);
			return newLine;
		}
	};
	
	
	public static class EvenAdder implements Function<Tuple2<String, Integer>, Tuple3<String, String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple3<String, String, Integer> call(Tuple2<String, Integer> line) throws Exception {
			Tuple3<String, String, Integer> newLine = new Tuple3<String, String, Integer>(line._1, "even", line._2);
			return newLine;
		}
	};
	
	
	public static class Keyer implements PairFunction<Tuple3<String, String, Integer>, String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Tuple2<String, Integer>> call(Tuple3<String, String, Integer> line) throws Exception {
			
			Tuple2<String, Tuple2<String, Integer>> newLine = new Tuple2<String, Tuple2<String, Integer>>(line._2(), 
					new Tuple2<String, Integer>(line._1(), line._3()));
			return newLine;
		}
	};
	
	
	public static class FilterOdd implements Function<Tuple2<String, Integer>,Boolean> {
		private static final long serialVersionUID = 1L;

		public Boolean call(Tuple2<String, Integer> line) throws Exception {
			Boolean isOdd = (Long.valueOf(line._1.split(" ")[0]) % 2) != 0;
			return isOdd;
		}
	};


	public static class FilterEven implements Function<Tuple2<String, Integer>,Boolean> {
		private static final long serialVersionUID = 1L;

		public Boolean call(Tuple2<String, Integer> line) throws Exception {
			Boolean isEven = (Long.valueOf(line._1.split(" ")[0]) % 2) == 0;
			return isEven;
		}
	};


	public static class MapperKafka implements Function<ConsumerRecord<String, String>, String> {
		private static final long serialVersionUID = 1L;

		public String call(ConsumerRecord<String, String> record) throws Exception {
			return record.value().toString();
		}
	};


	public static class NumberAdder implements Function<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> call(String line) {
			Tuple2<String, Integer> newLine = new Tuple2<String, Integer>(line, 1);
			return newLine;
		}
	};


	public static class Reducer implements Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> call(Tuple2<String, Integer> line1, Tuple2<String, Integer> line2) throws Exception {
			Long sum = Long.valueOf(line1._1.split(" ")[0]) + Long.valueOf(line2._1.split(" ")[0]);
			Long sumTS = Long.valueOf(line1._1.split(" ")[1]) + Long.valueOf(line2._1.split(" ")[1]);
			Tuple2<String, Integer> newLine = new Tuple2<String, Integer>(String.valueOf(sum) + " " + String.valueOf(sumTS), 
					line1._2 + line2._2);
			return newLine;
		}
	};


	public static class AverageCalculator implements Function<Tuple2<String, Tuple2<String, Integer>>, String> {
		private static final long serialVersionUID = 1L;

		public String call(Tuple2<String, Tuple2<String, Integer>> line) throws Exception {
			Long average = Long.valueOf(line._2._1.split(" ")[1]) / line._2._2;
			String result = String.valueOf(line._2._2) + " " + String.valueOf(average);
			return result;
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
			props.put("acks", "all");
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

}