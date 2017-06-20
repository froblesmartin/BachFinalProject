/**
 * @author  	Francisco Robles Martin
 * @date		June 2017
 * @project 	Computer Science bachelor's final project:
 * 				Comparison between Spark Streaming and Flink
 * @university	Technical University of Madrid
 */

package port;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class TimestampPort {

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
		DataStream<String> lines = env.socketTextStream("localhost", 9999).rescale();

		//Add timestamp and calculate the difference with the creation time
		DataStream<String> lineTS = lines.map(new TimestampAdder()).rescale();
		
		
		//Send the result to port 9998
		lineTS.writeToSocket("localhost", 9998, new SimpleStringSchema());

		env.execute("TimestampPort");
	}
	
	
	//Functions used in the program implementation:

	public static final class TimestampAdder implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		public String map(String line) throws Exception {
			Long currentTime = System.currentTimeMillis();
			String totalTime = String.valueOf(currentTime - Long.valueOf(line.split(" ")[1]));
			String newLine = line.concat(" " + String.valueOf(currentTime) + " " + totalTime + "\n");

			return newLine;
		}
	}
}
