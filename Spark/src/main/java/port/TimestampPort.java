/**
 * @author  	Francisco Robles Martin
 * @date		June 2017
 * @project 	Computer Science bachelor's final project:
 * 				Comparison between Spark Streaming and Flink
 * @university	Technical University of Madrid
 */

package port;

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
		if (args.length != 1) {
			System.err.println("Usage: TimestampPort <batch-interval (ms)>");
			System.exit(1);
		}

		
		//SPARK CONFIGURATION
		SparkConf sparkConf = new SparkConf().setAppName("TimestampPort");
		
		@SuppressWarnings("resource")
		JavaStreamingContext jStreamingContext = new JavaStreamingContext(sparkConf, 
				Durations.milliseconds(Integer.valueOf(args[1])));

		
		//MAIN PROGRAM
		JavaReceiverInputDStream<String> line = jStreamingContext.socketTextStream("localhost", 
				9999, StorageLevel.MEMORY_ONLY());		

		//Add timestamp and calculate the difference with the creation time
		JavaDStream<String> lineTS = line.map(new TimestampAdder());

		
		//Send the result to port 9998
		lineTS.foreachRDD(new NetworkPublisher());

		jStreamingContext.start();
		jStreamingContext.awaitTermination();
	}

	
	//Functions used in the program implementation:
	
	public static class TimestampAdder implements Function<String, String> {
		private static final long serialVersionUID = 1L;

		public String call(String line) {
			Long currentTime = System.currentTimeMillis();
			String totalTime = String.valueOf(currentTime - Long.valueOf(line.split(" ")[1]));
			String newLine = line.concat(" " + String.valueOf(currentTime) + " " + totalTime);

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