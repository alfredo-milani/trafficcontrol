package it.uniroma2.sdcc.sensorssimulator;

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

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
        while (true) {
            produce();

            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public String produce() {
        mobileId = ThreadLocalRandom.current().nextLong(1, 50);
        mobileTimestampUTC = System.currentTimeMillis();
        mobileLatitude = ThreadLocalRandom.current().nextDouble(0, 90 + 1);
        mobileLonditude = ThreadLocalRandom.current().nextDouble(0, 180 + 1);
        mobileSpeed = (short) ThreadLocalRandom.current().nextInt(0, 100 + 1);

        try {
            String jsonString = mapper.writeValueAsString(this);
            producer.send(new ProducerRecord<>(topicName, jsonString));
            return jsonString;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "Errore durante l'invio della tupla mobile";
        }
    }

    public MobileSensorProducer(KafkaProducer<String, String> producer, String topicName,
                                   int waitTime, Long intersectionId,
                                   Double semaphoreLatitude, Double semaphoreLonditude,
                                   Long semaphoreTimestampUTC, Short averageVehiclesSpeed) {
        this.producer = producer;
        this.topicName = topicName;
        this.waitTime = waitTime;

        this.mobileId = intersectionId;
        this.mobileTimestampUTC = semaphoreTimestampUTC;
        this.mobileLatitude = semaphoreLatitude;
        this.mobileLonditude = semaphoreLonditude;
        this.mobileSpeed = averageVehiclesSpeed;
    }

}
