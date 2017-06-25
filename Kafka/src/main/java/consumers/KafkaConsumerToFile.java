/**
 * @author  	Francisco Robles Martin
 * @date		June 2017
 * @project 	Computer Science bachelor's final project:
 * 				Comparison between Spark Streaming and Flink
 * @university	Technical University of Madrid
 */

package consumers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerToFile {

	public static void main(String[] args) throws IOException {
		if (args.length != 2){
			System.err.println("USAGE: KafkaConsumer <topic> <linesToFlush>");
			System.err.println("\t <linesToFlush> : the program will flush the data to the file every linesTuFlush");
			return;
		}

		
		//KAFKA CONSUMER PROPERTIES
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		//props.put("bootstrap.servers", "192.168.0.155:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		
		//VARIABLES
		long diff = 0;
		//long diff2 = 0;
		File f = new File("/home/fran/nfs/nfs/latency.txt");
		int flush = Integer.valueOf(args[1]);
		int flushWaiter = 0;
		FileWriter fw = new FileWriter(f, true);
		@SuppressWarnings("resource")
		BufferedWriter bw = new BufferedWriter(fw);


		//MAIN PROGRAM
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(args[0]));
		while (true) {
			
			ConsumerRecords<String, String> records = consumer.poll(100);
			
			for (ConsumerRecord<String, String> record : records) {
				//diff2 = System.currentTimeMillis() - record.timestamp();
				diff = record.timestamp() - Long.valueOf(record.value().split(" ")[1]);
				//bw.append(record.value() + " " + record.timestamp() + " " +  diff + " " + diff2 + "\n");
				bw.append(record.value() + " " + record.timestamp() + " " +  diff + "\n");
				flushWaiter++;
				
				if (flushWaiter == flush) {
					flushWaiter = 0;
					bw.flush();
				}
			}
		}
	}
}


