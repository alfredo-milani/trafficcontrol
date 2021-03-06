package it.uniroma2.sdcc.sensorSimulatorTest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ThreadLocalRandom;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.*;

public class MobileSensorProducer implements Runnable {

    @JsonProperty(MOBILE_ID)
    private Long mobileId;
    @JsonProperty(MOBILE_TIMESTAMP_UTC)
    private Long mobileTimestampUTC;
    @JsonProperty(MOBILE_LATITUDE)
    private Double mobileLatitude;
    @JsonProperty(MOBILE_LONGITUDE)
    private Double mobileLonditude;
    @JsonProperty(MOBILE_SPEED)
    private Short mobileSpeed;

    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaProducer<String, String> producer;
    private final String topicName;
    private final int waitTime;

    public MobileSensorProducer(KafkaProducer<String, String> producer, String topicName) {
        this(producer, topicName, 0);
    }

    public MobileSensorProducer(KafkaProducer<String, String> producer, String topicName, int waitTime) {
        this.producer = producer;
        this.topicName = topicName;
        this.waitTime = waitTime;
    }

    @SuppressWarnings({"InfiniteLoopStatement", "Duplicates"})
    @Override
    public void run() {
        while (true) {
            System.out.println(String.format("\t> Tupla inviata\t\t| %s |", produce()));

            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public String produce() {
        mobileId = ThreadLocalRandom.current().nextLong(1, 2000);
        mobileTimestampUTC = System.currentTimeMillis();
        /*mobileLatitude = ThreadLocalRandom.current().nextDouble(0, 90 + 1);
        mobileLonditude = ThreadLocalRandom.current().nextDouble(0, 180 + 1);*/
        mobileLatitude = ThreadLocalRandom.current().nextDouble(40.761, 40.762);
        mobileLonditude = -73.997357;
        mobileSpeed = (short) ThreadLocalRandom.current().nextInt(0, 150 + 1);

        try {
            String jsonString = mapper.writeValueAsString(this);
            producer.send(new ProducerRecord<>(topicName, jsonString));
            return jsonString;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "Errore durante l'invio della tupla mobile";
        }
    }

}
