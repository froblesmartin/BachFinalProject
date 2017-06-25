/**
 * @author  	Francisco Robles Martin
 * @date		June 2017
 * @project 	Computer Science bachelor's final project:
 * 				Comparison between Spark Streaming and Flink
 * @university	Technical University of Madrid
 */

package producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProdIncremental {

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 5){
			System.err.println("USAGE: KafkaProdIncremental <topic> <start (ns)> <finish (ns)> <speedUpEvery <s> <reduceEach (%)>");
			return;
		}

		//KAFKA PRODUCER CONFIGURATION
		Properties props = new Properties();
		//props.put("bootstrap.servers", "localhost:9092");  for using in the same machine and override network usage
		props.put("bootstrap.servers", "192.168.0.155:9092");
		props.put("acks", "0");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 0);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


		//VARIABLES
		long i = 0;
		long delayNS = Long.valueOf(args[1]);
		long delayMin = Long.valueOf(args[2]);
		double percentageToReduce = Double.valueOf(args[4])/100;
		long timeToReduce = 1000000000 * Long.valueOf(args[3]);
		long time1 = System.nanoTime();


		//MAIN PROGRAM WITH BUSY DELAY
		@SuppressWarnings("resource")
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		
		while(true) {
			producer.send(new ProducerRecord<String, String>(args[0], i++ + " " + 
					Long.toString(System.currentTimeMillis())));

			long start = System.nanoTime();
			
			if (delayNS > delayMin) {
				if (start - time1 > timeToReduce) {
					time1 = start;
					delayNS -= delayNS * percentageToReduce;
				}
			}
			
			while (System.nanoTime() - start < delayNS);
		}

	}
}
