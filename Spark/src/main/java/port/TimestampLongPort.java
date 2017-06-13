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
		if (args.length != 2) {
			System.err.println("Usage: TimestampLongPort <port> <batch-interval (ms)>");
			System.err.println("\t <port> : port to read from");
			System.exit(1);
		}

		
		//SPARK CONFIGURATION
		SparkConf sparkConf = new SparkConf().setAppName("TimestampLongPort");
		
		@SuppressWarnings("resource")
		JavaStreamingContext jStreamingContext = new JavaStreamingContext(sparkConf, 
				Durations.milliseconds(Integer.valueOf(args[1])));

		
		//MAIN PROGRAM
		JavaReceiverInputDStream<String> line = jStreamingContext.socketTextStream("localhost", 
				Integer.valueOf(args[0]), StorageLevel.MEMORY_ONLY());		

		JavaDStream<String> windowedLine = line.window(Durations.seconds(5), Durations.seconds(4));
		
		JavaDStream<Tuple2<String, Integer>> wL_Num = windowedLine.map(new NumberAdder());
				
		JavaDStream<Tuple2<String, Integer>> wL_Num_Reduced = wL_Num.reduce(new Reducer());
		
		JavaDStream<String> wL_Num_Average = wL_Num_Reduced.map(new AverageCalculator());
		
		JavaDStream<String> wL_MulOf5 = windowedLine.filter(new Filter5());
		
		JavaDStream<String> wL_MulOf5_Count = wL_MulOf5.union(wL_Num_Average);
		
		JavaDStream<String> lineTS = wL_MulOf5_Count.map(new TimestampAdder());
		
		lineTS.foreachRDD(new NetworkPublisher());

		jStreamingContext.start();
		jStreamingContext.awaitTermination();
	}

	
	//Functions used in the program implementation:
	
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
		
	}
	
	
	public static class AverageCalculator implements Function<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		public String call(Tuple2<String, Integer> line) throws Exception {
			Long average = Long.valueOf(line._1.split(" ")[1]) / line._2;
			String result = line._1.split(" ")[0] + " " + String.valueOf(average);
			return result;
		}
	};
	
	
	public static class Filter5 implements Function<String,Boolean> {
		private static final long serialVersionUID = 1L;

		public Boolean call(String line) throws Exception {
			Boolean multipleOf5 = (Integer.valueOf(line.split(" ")[0]) % 5 == 0);
			return multipleOf5;
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