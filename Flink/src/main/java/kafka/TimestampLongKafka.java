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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class TimestampLongKafka {

	public static void main(String[] args) throws Exception {
		if (args.length != 1){
			System.err.println("USAGE: TimestampLongKafka <topic>");
			return;
		}

		
		//FLINK CONFIGURATION
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		
		//KAFKA CONSUMER CONFIGURATION
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "192.168.0.155:9092");
		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(args[0], new SimpleStringSchema(), properties);

		
		//MAIN PROGRAM
		DataStream<String> lines = env.addSource(myConsumer).setParallelism(4);

		DataStream<String> lines11 = lines.filter(new Filter11()).setParallelism(4);

		DataStream<String> lines00 = lines.filter(new Filter00()).setParallelism(4);

		DataStream<String> lines00and11 = lines00.union(lines11);

		DataStream<String> lineTS = lines00and11.map(new TimestampAdder()).setParallelism(4);

		lineTS.writeToSocket("192.168.0.155", 9998, new SimpleStringSchema()).setParallelism(4);

		env.execute("TimestampLongKafka");
	}

	
	//Functions used in the program implementation:
	
	public static final class Filter11 implements FilterFunction<String> {
		private static final long serialVersionUID = 1L;

		public boolean filter(String line) throws Exception {
			String[] tuple = line.split(" ");
			Boolean is11 = (Integer.valueOf(tuple[0]) == 11);
			return is11;
		}
	}

	
	public static final class Filter00 implements FilterFunction<String> {
		private static final long serialVersionUID = 1L;

		public boolean filter(String line) throws Exception {
			String[] tuple = line.split(" ");
			Boolean is00 = (Integer.valueOf(tuple[0]) == 00);
			return is00;
		}
	}

	
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
