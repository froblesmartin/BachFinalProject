/**
 * @author  	Francisco Robles Martin
 * @date		June 2017
 * @project 	Computer Science bachelor's final project:
 * 				Comparison between Spark Streaming and Flink
 * @university	Technical University of Madrid
 */

package port;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class TimestampLongPort {

	public static void main(String[] args) throws Exception {

		if (args.length != 2){
			System.err.println("USAGE: TimestampPort <checkpointing> <checkpointing time (ms)>");
			System.err.println("\t <checkpointing>: [0|1]");
			return;
		}


		//FLINK CONFIGURATION
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(4);

		if (Integer.valueOf(args[0]) == 1) { 
			env.enableCheckpointing(Integer.valueOf(args[1]));
			env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
			env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
			env.setStateBackend(new FsStateBackend("file:///home/fran/nfs/nfs/checkpoints/flink"));
		}

		//MAIN PROGRAM
		DataStream<String> line = env.socketTextStream("localhost", 9999);

		//Add 1 to each line
		DataStream<Tuple2<String, Integer>> line_Num = line.map(new NumberAdder());

		//Filter Odd numbers
		DataStream<Tuple2<String, Integer>> line_Num_Odd = line_Num.filter(new FilterOdd());
		DataStream<Tuple3<String, String, Integer>> line_Num_Odd_2 = line_Num_Odd.map(new OddAdder());

		//Filter Even numbers
		DataStream<Tuple2<String, Integer>> line_Num_Even = line_Num.filter(new FilterEven());
		DataStream<Tuple3<String, String, Integer>> line_Num_Even_2 = line_Num_Even.map(new EvenAdder());

		//Join Even and Odd
		DataStream<Tuple3<String, String, Integer>> line_Num_U = line_Num_Odd_2.union(line_Num_Even_2);

		//Change to KeyedStream
		KeyedStream<Tuple3<String, String, Integer>, Tuple> line_Num_U_K = line_Num_U.keyBy(1);

		//Tumbling windows every 2 seconds
		WindowedStream<Tuple3<String, String, Integer>, Tuple, TimeWindow> windowedLine_Num_U_K = line_Num_U_K
				.window(TumblingProcessingTimeWindows.of(Time.seconds(2)));

		//Reduce to one line with the sum
		DataStream<Tuple3<String, String, Integer>> wL_Num_U_Reduced = windowedLine_Num_U_K.reduce(new Reducer());

		//Calculate the average of the elements summed
		DataStream<String> wL_Average = wL_Num_U_Reduced.map(new AverageCalculator());

		//Add timestamp and calculate the difference with the average
		DataStream<String> averageTS = wL_Average.map(new TimestampAdder());

		//Send the result to port 9998
		averageTS.writeToSocket("localhost", 9998, new SimpleStringSchema());

		env.execute("TimestampLongPort");
	}


	//Functions used in the program implementation:

	public static class OddAdder implements MapFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple3<String, String, Integer> map(Tuple2<String, Integer> line) throws Exception {
			Tuple3<String, String, Integer> newLine = new Tuple3<String, String, Integer>(line.f0, "odd", line.f1);
			return newLine;
		}
	};
	
	
	public static class EvenAdder implements MapFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple3<String, String, Integer> map(Tuple2<String, Integer> line) throws Exception {
			Tuple3<String, String, Integer> newLine = new Tuple3<String, String, Integer>(line.f0, "even", line.f1);
			return newLine;
		}
	};
	
	
	public static class FilterOdd implements FilterFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public boolean filter(Tuple2<String, Integer> line) throws Exception {
			Boolean isOdd = (Long.valueOf(line.f0.split(" ")[0]) % 2) != 0;
			return isOdd;
		}
	};
	
	
	public static class FilterEven implements FilterFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public boolean filter(Tuple2<String, Integer> line) throws Exception {
			Boolean isEven = (Long.valueOf(line.f0.split(" ")[0]) % 2) == 0;
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
	
	
	public static class Reducer implements ReduceFunction<Tuple3<String, String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> line1,
				Tuple3<String, String, Integer> line2) throws Exception {
			Long sum = Long.valueOf(line1.f0.split(" ")[0]) + Long.valueOf(line2.f0.split(" ")[0]);
			Long sumTS = Long.valueOf(line1.f0.split(" ")[1]) + Long.valueOf(line2.f0.split(" ")[1]);
			Tuple3<String, String, Integer> newLine = new Tuple3<String, String, Integer>(String.valueOf(sum) +
					" " + String.valueOf(sumTS), line1.f1, line1.f2 + line2.f2);
			return newLine;
		}
	};


	public static class AverageCalculator implements MapFunction<Tuple3<String, String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		public String map(Tuple3<String, String, Integer> line) throws Exception {
			Long average = Long.valueOf(line.f0.split(" ")[1]) / line.f2;
			String result = String.valueOf(line.f2) + " " + String.valueOf(average);
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
