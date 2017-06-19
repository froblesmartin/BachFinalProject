/**
 * @author  	Francisco Robles Martin
 * @date		June 2017
 * @project 	Computer Science bachelor's final project:
 * 				Comparison between Spark Streaming and Flink
 * @university	Technical University of Madrid
 */

package kafka;

import java.util.Properties;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import scala.Tuple2;

public class ThroughputKafka {

	public static void main(String[] args) throws Exception {
		if (args.length != 1){
			System.err.println("USAGE: ThroughputKafka <topic>");
			return;
		}

		
		//FLINK CONFIGURATION
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		//env.setParallelism(4);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		
		//KAFKA CONSUMER CONFIGURATION
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "192.168.0.155:9092");
		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(args[0], new SimpleStringSchema(), properties);
		
		
		//KAFKA PRODUCER
		Properties producerConfig = new Properties();
		producerConfig.setProperty("bootstrap.servers", "192.168.0.155:9092");
		producerConfig.setProperty("acks", "0");
		//producerConfig.put("retries", 0);
		//producerConfig.put("batch.size", 16384);
		producerConfig.setProperty("linger.ms", "0");
		//producerConfig.put("buffer.memory", 33554432);
		//producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		
		//MAIN PROGRAM
		//Read from Kafka
		DataStream<String> line = env.addSource(myConsumer).setParallelism(4);
		
		/*
		 * This part is just to consume CPU as all the changes results in nothing in the end
		 */
		DataStream<String> lineSum = line.map(new WhileSumAllNumbers()).setParallelism(4);
		
		DataStream<String> line2 = lineSum.map(new RemoveSumAllNumbers()).setParallelism(4);
				
		

		//Add 1 to each line
		DataStream<Tuple2<String, Integer>> line_Num = line2.map(new NumberAdder()).setParallelism(4);
		
		//Filted Odd numbers
		DataStream<Tuple2<String, Integer>> line_Num_Odd = line_Num.filter(new FilterOdd()).setParallelism(4);
		
		//Filter Even numbers
		DataStream<Tuple2<String, Integer>> line_Num_Even = line_Num.filter(new FilterEven()).setParallelism(4);
		
		//Join Even and Odd
		DataStream<Tuple2<String, Integer>> line_Num_U = line_Num_Odd.union(line_Num_Even);
		
		//Tumbling windows every 2 seconds
		AllWindowedStream<Tuple2<String, Integer>, TimeWindow> windowedLine_Num_U = line_Num_U
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(2)));
		
		//Reduce to one line with the sum
		DataStream<Tuple2<String, Integer>> wL_Num_U_Reduced = windowedLine_Num_U.reduce(new Reducer());
		
		//Calculate the average of the elements summed
		DataStream<String> wL_Average = wL_Num_U_Reduced.map(new AverageCalculator()).setParallelism(1);
		
		//Add timestamp and calculate the difference with the average
		DataStream<String> averageTS = wL_Average.map(new TimestampAdder()).setParallelism(1);
		

		//Send the result to Kafka
		FlinkKafkaProducer010Configuration<String> myProducerConfig = (FlinkKafkaProducer010Configuration<String>) FlinkKafkaProducer010
				.writeToKafkaWithTimestamps(averageTS, "testRes", new SimpleStringSchema(), producerConfig).setParallelism(1);
		
		myProducerConfig.setWriteTimestampToKafka(true);

		env.execute("ThroughputKafka");
		
	}

	
	//Functions used in the program implementation:
	
	public static class WhileSumAllNumbers implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		public String map(String line) {
			int sumNumbers = 0;
			for (int i = 1; i <= line.length(); i++) {
				if (line.substring(i-1, i).matches("[-+]?\\d*\\.?\\d+")) {
					sumNumbers += Integer.valueOf(line.substring(i-1, i));
				}
			}
			String newLine = line.concat(" " + String.valueOf(sumNumbers));
			return newLine;
		}
	};
	
	
	public static class RemoveSumAllNumbers implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		public String map(String line) {
			String newLine = line.split(" ")[0] + " " + line.split(" ")[1];
			return newLine;
		}
	};
	
	
	public static class NumberAdder implements MapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> map(String line) {
			Tuple2<String, Integer> newLine = new Tuple2<String, Integer>(line, 1);
			return newLine;
		}
	};
	
	
	public static class FilterOdd implements FilterFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public boolean filter(Tuple2<String, Integer> line) throws Exception {
			Boolean isOdd = (Long.valueOf(line._1.split(" ")[0]) % 2) != 0;
			return isOdd;
		}
	};
	
	
	public static class FilterEven implements FilterFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public boolean filter(Tuple2<String, Integer> line) throws Exception {
			Boolean isEven = (Long.valueOf(line._1.split(" ")[0]) % 2) == 0;
			return isEven;
		}
	};


	public static class Reducer implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> line1, Tuple2<String, Integer> line2) throws Exception {
			Long sum = Long.valueOf(line1._1.split(" ")[0]) + Long.valueOf(line2._1.split(" ")[0]);
			Long sumTS = Long.valueOf(line1._1.split(" ")[1]) + Long.valueOf(line2._1.split(" ")[1]);
			Tuple2<String, Integer> newLine = new Tuple2<String, Integer>(String.valueOf(sum) + " " + String.valueOf(sumTS), 
					line1._2 + line2._2);
			return newLine;
		}
	};


	public static class AverageCalculator implements MapFunction<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		public String map(Tuple2<String, Integer> line) throws Exception {
			Long average = Long.valueOf(line._1.split(" ")[1]) / line._2;
			String result = String.valueOf(line._2) + " " + String.valueOf(average);
			return result;
		}
	};
	
	
	public static final class TimestampAdder implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		public String map(String line) throws Exception {
			Long currentTime = System.currentTimeMillis();
			String totalTime = String.valueOf(currentTime - Long.valueOf(line.split(" ")[1]));
			String newLine = line.concat(" " + String.valueOf(currentTime) + " " + totalTime);

			return newLine;
		}
	};
}

