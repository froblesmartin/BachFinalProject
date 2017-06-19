/**
 * @author  	Francisco Robles Martin
 * @date		June 2017
 * @project 	Computer Science bachelor's final project:
 * 				Comparison between Spark Streaming and Flink
 * @university	Technical University of Madrid
 */

package port;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import scala.Tuple2;

public class TimestampLongPort {

	public static void main(String[] args) throws Exception {

//		if (args.length != 1){
//			System.err.println("USAGE: TimestampLongPort <port>");
//			return;
//		}


		//FLINK CONFIGURATION
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		//MAIN PROGRAM
		DataStream<String> line = env.socketTextStream("localhost", 9999);

		//Add 1 to each line
		DataStream<Tuple2<String, Integer>> line_Num = line.map(new NumberAdder());

		//Filted Odd numbers
		DataStream<Tuple2<String, Integer>> line_Num_Odd = line_Num.filter(new FilterOdd());

		//Filter Even numbers
		DataStream<Tuple2<String, Integer>> line_Num_Even = line_Num.filter(new FilterEven());

		//Join Even and Odd
		DataStream<Tuple2<String, Integer>> line_Num_U = line_Num_Odd.union(line_Num_Even);

		//Tumbling windows every 2 seconds
		AllWindowedStream<Tuple2<String, Integer>, TimeWindow> windowedLine_Num_U = line_Num_U
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(2)));

		//Reduce to one line with the sum
		DataStream<Tuple2<String, Integer>> wL_Num_U_Reduced = windowedLine_Num_U.reduce(new Reducer());

		//Calculate the average of the elements summed
		DataStream<String> wL_Average = wL_Num_U_Reduced.map(new AverageCalculator());

		//Add timestamp and calculate the difference with the average
		DataStream<String> averageTS = wL_Average.map(new TimestampAdder());


		//Send the result to port 9998
		averageTS.writeToSocket("localhost", 9998, new SimpleStringSchema());

		env.execute("TimestampLongPort");
	}


	//Functions used in the program implementation:

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


	public static class NumberAdder implements MapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> map(String line) {
			Tuple2<String, Integer> newLine = new Tuple2<String, Integer>(line, 1);
			return newLine;
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
			String newLine = line.concat(" " + String.valueOf(currentTime) + " " + totalTime + "\n");

			return newLine;
		}
	};
	
}
