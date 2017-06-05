/**
 * @author  Francisco Robles Martin
 * @date	June 2017
 */

package com.fran.flink.flink1;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class TimestampLongPort {

	public static void main(String[] args) throws Exception {

		if (args.length != 1){
			System.err.println("USAGE:\nTimestampLongPort <port>");
			return;
		}

		Integer port = Integer.parseInt(args[0]);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		DataStream<String> lines = env.socketTextStream("localhost", port);

		DataStream<String> lines11 = lines.filter(new Filter11());
		
		DataStream<String> lines00 = lines.filter(new Filter00());
		
		DataStream<String> lines00and11 = lines00.union(lines11);
		
		DataStream<String> lineTS = lines00and11.map(new TimestampAdder());
		
		lineTS.writeToSocket("localhost", 9998, new SimpleStringSchema());

		env.execute("Java TimestampPort");
	}

	public static final class Filter11 implements FilterFunction<String> {
		private static final long serialVersionUID = 1L;

		public boolean filter(String line) throws Exception {
			String[] tuple = line.split(" ");
			Boolean has11 = (Integer.valueOf(tuple[0]) == 11);
			return has11;
		}
	}
	
	public static final class Filter00 implements FilterFunction<String> {
		private static final long serialVersionUID = 1L;

		public boolean filter(String line) throws Exception {
			String[] tuple = line.split(" ");
			Boolean has11 = (Integer.valueOf(tuple[0]) == 00);
			return has11;
		}
	}
	
	public static final class TimestampAdder implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		public String map(String line) throws Exception {
			String[] tuple = line.split(" ");
			String totalTime = String.valueOf(System.currentTimeMillis() - Long.valueOf(tuple[1]));
			String newLine = line.concat(" " + String.valueOf(System.currentTimeMillis()) + " " + totalTime + "\n");
			return newLine;
		}

	}

}
