/**
 * @author  	Francisco Robles Martin
 * @date		June 2017
 * @project 	Computer Science bachelor's final project:
 * 				Comparison between Spark Streaming and Flink
 * @university	Technical University of Madrid
 */

package producers;

import java.util.Properties;
//import java.util.Random;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class myKafkaProducer {

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 3){
			System.err.println("USAGE: KafkaProducer <topic> <ns> <linger.ms> <linger mode>");
			System.err.println("\t If <ns> = 0 --> Delay disabled");
			System.err.println("\t <linger mode> = [BUSY|LOCK]");
			return;
		}
		
		
		//KAFKA PRODUCER CONFIGURATION
		int batch_size = 16384;
		//		if (args.length == 4) {
		//			batch_size = Integer.valueOf(args[3]);
		//		}

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.155:9092");
		props.put("acks", "0");
		props.put("retries", 0);
		props.put("batch.size", batch_size);
		props.put("linger.ms", Integer.valueOf(args[2]));
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		
		//VARIABLES
		long i = 0;
		long delayNS = Long.valueOf(args[1]);

		
		//MAIN PROGRAM WITH DIFFERENT BEHAVIOURS IMPROVED FOR BEST PERFORMANCE
		
		if (delayNS == 0) {	
			//NO DELAY
			@SuppressWarnings("resource")
			Producer<String, String> producer = new KafkaProducer<>(props);
			while(true) {
				producer.send(new ProducerRecord<String, String>(args[0], i++ + " " + 
						Long.toString(System.currentTimeMillis())));
			}
		} else if (args[3].contains("BUSY")) {
			//BUSY DELAY
			@SuppressWarnings("resource")
			Producer<String, String> producer = new KafkaProducer<>(props);
			while(true) {
				producer.send(new ProducerRecord<String, String>(args[0], i++ + " " + 
						Long.toString(System.currentTimeMillis())));
				//LockSupport.parkNanos(delayNS);

				long start = System.nanoTime();
				while (System.nanoTime() - start < delayNS);
			}
		} else if (args[3].contains("LOCK")) {
			@SuppressWarnings("resource")
			Producer<String, String> producer = new KafkaProducer<>(props);
			while(true) {
				producer.send(new ProducerRecord<String, String>(args[0], i++ + " " + 
						Long.toString(System.currentTimeMillis())));
				LockSupport.parkNanos(delayNS);
			}
		} else {
			System.out.println("Something wrong");
		}
	}
}
