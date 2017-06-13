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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class TimestampKafka {

	public static void main(String[] args) throws Exception {
		if (args.length != 1){
			System.err.println("USAGE: TimestampKafka <topic>");
			return;
		}
		
		
		//Flink main configuration
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		//KAFKA CONSUMER
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
		DataStream<String> lines = env.addSource(myConsumer);
		
		DataStream<String> lineTS = lines.map(new TimestampAdder()).rescale();
		
		FlinkKafkaProducer010Configuration<String> myProducerConfig = (FlinkKafkaProducer010Configuration<String>) FlinkKafkaProducer010
				.writeToKafkaWithTimestamps(lineTS, "testRes", new SimpleStringSchema(), producerConfig).setParallelism(2);
		
		
		myProducerConfig.setWriteTimestampToKafka(true);
		
		//lineTS.writeToSocket("192.168.0.155", 9998, new SimpleStringSchema()).setParallelism(1);

		env.execute("TimestampKafka");
	}

	
	//Functions used in the program implementation:
	
	public static final class TimestampAdder implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		public String map(String line) throws Exception {
			Long currentTime = System.currentTimeMillis();
			String totalTime = String.valueOf(currentTime - Long.valueOf(line.split(" ")[1]));
			String newLine = line.concat(" " + String.valueOf(currentTime) + " " + totalTime);

			return newLine;
		}
	}
	
}
