package it.uniroma2.sdcc.lampsystem;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.*;


public class SensorThread implements Runnable {
    private final Sensor sensor;
    private final KafkaProducer<String, String> producer;
    private KafkaConsumer<String,String> consumer;
    private ObjectMapper mapper = new ObjectMapper();

    public SensorThread(Sensor sensor) {
        this.sensor = sensor;

        //initialize producerTest
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("key.serializer", StringSerializer.class);
        producerProperties.put("value.serializer", StringSerializer.class);

        producer = new KafkaProducer<String, String>(producerProperties);

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", "localhost:9092");
        consumerProperties.put("group.id", String.valueOf(Thread.currentThread().getId()));
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("auto.offset.reset", "earliest");
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());

        /*String topic;

        if (sensor.getType().equals("park"))
            topic = "park" + sensor.getZoneID();
        else if (sensor.getType().equals("bus_stop"))
            topic = "bus_stop" + sensor.getZoneID();
        else
            topic = "street" + sensor.getLatitude();*/

        consumer = new KafkaConsumer<String, String>(consumerProperties);
        consumer.subscribe(Collections.singletonList(KafkaParams.MONITORING_SOURCE));
    }

    private long setBrightnessByHour() {

        Date date = new Date();   // given date
        Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
        calendar.setTime(date);   // assigns calendar to given date
        int hour = calendar.get(Calendar.HOUR_OF_DAY); // gets hour in 24h format
        if ((hour >= 21 && hour <= 0) || (hour > 0 && hour <=6))
            return 0;
        if (hour > 6 && hour <= 9)
            return 50;
        if (hour >=10 && hour <= 15)
            return 100;
        if (hour >=16 && hour <= 17)
            return 70;
        else
            return 30;
    }



    //check if lamps has been repaired

    private boolean checkReparation() {
        Random random = new Random(System.currentTimeMillis());
        int num = random.nextInt(100);
        return num < 5;
    }

    private int getNumberOfVehicle() {
        Random random = new Random(System.currentTimeMillis());
        return random.nextInt(300);
    }

    private Float getSpeed() {
        Random random = new Random(System.currentTimeMillis());
        Integer i =random.nextInt(50);
        return i.floatValue();
    }

    private boolean getState() {
        Random random = new Random(System.currentTimeMillis());
        int num = random.nextInt(100);
        return num < 5;
    }




    public void run() {

        while (true) {
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            sensor.setStateGreen(!getState());
            sensor.setStateRed(!getState());
            sensor.setStateYellow(!getState());

            sensor.setNumberVehicle(getNumberOfVehicle());
            sensor.setMeanSpeed(getSpeed());

            //for each loop, a sensor get all messages on kafka and use only information it needs


            ConsumerRecords<String, String> records = consumer.poll(100);


            int size = 0;

            for (ConsumerRecord<String, String> record : records) {
                String jsonTuple = record.value();

                JsonNode jsonNode = null;
                try {
                    jsonNode = mapper.readTree(jsonTuple);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


            try {
                String jsonStringLamp = mapper.writeValueAsString(sensor);
                System.out.println(jsonStringLamp);
                producer.send(new ProducerRecord<String, String>("monitoring_source", jsonStringLamp));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }
}
