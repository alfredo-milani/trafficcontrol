package it.uniroma2.sdcc.trafficcontrol.producerTest;



//import util.properties packages

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

//import simple producerTest packages
//import KafkaProducer packages
//import ProducerRecord packages

//Create java class named “SimpleProducer”
public class SimpleProducer {

    public static void main(String[] args) {

        /**
         * prima fai partire da terminale zookeeper e kafka
         */

        System.out.println("insert topic name");

        Scanner scanner = new Scanner(System.in);

        //Assign topicName to string variable
        String topicName = scanner.nextLine();

        // create instance for properties to access producerTest configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producerTest requests.
        props.put("acks", "all");

        //If the request fails, the producerTest can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producerTest for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), "{'id':'1245', 'timestamp':'11:10:11'}"));
            System.out.println(Integer.toString(i));
        }
        System.out.println("Message sent successfully");
        producer.close();
    }
}
