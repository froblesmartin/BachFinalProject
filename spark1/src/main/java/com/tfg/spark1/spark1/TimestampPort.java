package com.tfg.spark1.spark1;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

public final class TimestampPort {

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: TimestampPort <port> <batch-interval");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setMaster("spark://192.168.0.155:7077").setAppName("TimestampPort");
		@SuppressWarnings("resource")
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(Integer.valueOf(args[1])));

		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", Integer.valueOf(args[0]),StorageLevel.MEMORY_ONLY());		

		JavaDStream<String> lineTS = lines.map(new TimestampAdder());

		lineTS.foreachRDD(new NetworkPublisher());

		jssc.start();
		jssc.awaitTermination();
	}

	//Functions used in the program implementations:

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
					Socket mySocket = new Socket("localhost", 9998);
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