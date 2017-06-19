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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;

public final class TimestampLongPort {

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: TimestampLongPort <port> <batch-interval (ms)>");
			System.exit(1);
		}


		//SPARK CONFIGURATION
		SparkConf sparkConf = new SparkConf().setAppName("TimestampLongPort");

		@SuppressWarnings("resource")
		JavaStreamingContext jStreamingContext = new JavaStreamingContext(sparkConf, 
				Durations.milliseconds(Integer.valueOf(args[1])));


		//MAIN PROGRAM
		JavaReceiverInputDStream<String> line = jStreamingContext.socketTextStream("localhost", 
				9999, StorageLevel.MEMORY_ONLY());		

		//Add 1 to each line
		JavaDStream<Tuple2<String, Integer>> line_Num = line.map(new NumberAdder());

		//Filted Odd numbers
		JavaDStream<Tuple2<String, Integer>> line_Num_Odd = line_Num.filter(new FilterOdd());

		//Filter Even numbers
		JavaDStream<Tuple2<String, Integer>> line_Num_Even = line_Num.filter(new FilterEven());

		//Join Even and Odd
		JavaDStream<Tuple2<String, Integer>> line_Num_U = line_Num_Odd.union(line_Num_Even);

		//Tumbling windows every 2 seconds
		JavaDStream<Tuple2<String, Integer>> windowedLine_Num_U = line_Num_U
				.window(Durations.seconds(2), Durations.seconds(2));

		//Reduce to one line with the sum
		JavaDStream<Tuple2<String, Integer>> wL_Num_U_Reduced = windowedLine_Num_U.reduce(new Reducer());

		//Calculate the average of the elements summed
		JavaDStream<String> wL_Average = wL_Num_U_Reduced.map(new AverageCalculator());

		//Add timestamp and calculate the difference with the average
		JavaDStream<String> averageTS = wL_Average.map(new TimestampAdder());


		//Send the result to the port 9998
		averageTS.foreachRDD(new NetworkPublisher());


		jStreamingContext.start();
		jStreamingContext.awaitTermination();
	}


	//Functions used in the program implementation:

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


	public static class AverageCalculator implements Function<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		public String call(Tuple2<String, Integer> line) throws Exception {
			Long average = Long.valueOf(line._1.split(" ")[1]) / line._2;
			String result = String.valueOf(line._2) + " " + String.valueOf(average);
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