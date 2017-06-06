/**
 * @author  Francisco Robles Martin
 * @date	June 2017
 */

package com.fran.flink.flink1;

import java.util.Properties;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class TimestampKafka {

	public static void main(String[] args) throws Exception {

		if (args.length != 1){
			System.err.println("USAGE:\nTimestampKafka <topic>");
			return;
		}
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "192.168.0.155:9092");
		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(args[0], new SimpleStringSchema(), properties);
		
		DataStream<String> lines = env.addSource(myConsumer).setParallelism(2);
		
		DataStream<String> lineTS = lines.map(new TimestampAdder()).setParallelism(4);
		
		lineTS.writeToSocket("192.168.0.155", 9998, new SimpleStringSchema()).setParallelism(1);

		env.execute("TimestampKafka");
	}

	public static final class TimestampAdder implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		public String map(String line) throws Exception {
			String[] tuple = line.split(" ");
			String totalTime = String.valueOf(System.currentTimeMillis() - Long.valueOf(tuple[1]));
			String newLine = line.
					concat(" " + String.valueOf(System.currentTimeMillis()) + " " + totalTime + "\n");
			return newLine;
		}
	}
	
}
