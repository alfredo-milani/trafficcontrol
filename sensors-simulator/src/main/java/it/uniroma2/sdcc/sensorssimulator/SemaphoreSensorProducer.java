package it.uniroma2.sdcc.sensorssimulator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ThreadLocalRandom;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;


public class SemaphoreSensorProducer implements Runnable {

    @JsonProperty(INTERSECTION_ID)
    private Long intersectionId;
    @JsonProperty(SEMAPHORE_ID)
    private Long semaphoreId;
    @JsonProperty(SEMAPHORE_LATITUDE)
    private Double semaphoreLatitude;
    @JsonProperty(SEMAPHORE_LONGITUDE)
    private Double semaphoreLonditude;
    @JsonProperty(SEMAPHORE_TIMESTAMP_UTC)
    private Long semaphoreTimestampUTC;
    @JsonProperty(GREEN_LIGHT_DURATION)
    private Short greenLightDuration;
    @JsonProperty(GREEN_LIGHT_STATUS)
    private Byte greenLightStatus;
    @JsonProperty(YELLOW_LIGHT_STATUS)
    private Byte yellowLightStatus;
    @JsonProperty(RED_LIGHT_STATUS)
    private Byte redLightStatus;
    @JsonProperty(VEHICLES)
    private Short vehiclesPerSecond;
    @JsonProperty(AVERAGE_VEHICLES_SPEED)
    private Short averageVehiclesSpeed;

    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaProducer<String, String> producer;
    private final String topicName;
    private final int waitTime;

    public SemaphoreSensorProducer(KafkaProducer<String, String> producer, String topicName) {
        this(producer, topicName, 0);
    }

    public SemaphoreSensorProducer(KafkaProducer<String, String> producer, String topicName, int waitTime) {
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
        intersectionId = ThreadLocalRandom.current().nextLong(1, 50);
        semaphoreId = ThreadLocalRandom.current().nextLong(1, 50);
        semaphoreLatitude = ThreadLocalRandom.current().nextDouble(0, 90 + 1);
        semaphoreLonditude = ThreadLocalRandom.current().nextDouble(0, 180 + 1);
        semaphoreTimestampUTC = ThreadLocalRandom.current().nextLong(0, 10000000 + 1);
        semaphoreTimestampUTC = System.currentTimeMillis();
        greenLightDuration = (short) ThreadLocalRandom.current().nextInt(0, 300 + 1);
        greenLightStatus = (byte) ThreadLocalRandom.current().nextInt(0, 127 + 1);
        yellowLightStatus = (byte) ThreadLocalRandom.current().nextInt(0, 127 + 1);
        redLightStatus = (byte) ThreadLocalRandom.current().nextInt(0, 127 + 1);
            /*greenLightStatus = Byte.MAX_VALUE;
            yellowLightStatus = Byte.MAX_VALUE;
            redLightStatus = Byte.MAX_VALUE;*/
        vehiclesPerSecond = (short) ThreadLocalRandom.current().nextInt(0, 150 + 1);
        averageVehiclesSpeed = (short) ThreadLocalRandom.current().nextInt(0, 150 + 1);

        try {
            String jsonStringLamp = mapper.writeValueAsString(this);
            // StartProducer.getLOGGER().log(Level.INFO, jsonStringLamp);
            producer.send(new ProducerRecord<>(topicName, jsonStringLamp));
            return jsonStringLamp;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "Errore durante l'invio della tupla";
        }
    }

    public SemaphoreSensorProducer(KafkaProducer<String, String> producer, String topicName,
                                   int waitTime,
                                   Long intersectionId, Long semaphoreId,
                                   Double semaphoreLatitude, Double semaphoreLonditude,
                                   Long semaphoreTimestampUTC, Short greenLightDuration,
                                   Byte greenLightStatus, Byte yellowLightStatus,
                                   Byte redLightStatus, Short vehiclesPerSecond,
                                   Short averageVehiclesSpeed) {
        this.producer = producer;
        this.topicName = topicName;
        this.waitTime = waitTime;

        this.intersectionId = intersectionId;
        this.semaphoreId = semaphoreId;
        this.semaphoreLatitude = semaphoreLatitude;
        this.semaphoreLonditude = semaphoreLonditude;
        this.semaphoreTimestampUTC = semaphoreTimestampUTC;
        this.greenLightDuration = greenLightDuration;
        this.greenLightStatus = greenLightStatus;
        this.yellowLightStatus = yellowLightStatus;
        this.redLightStatus = redLightStatus;
        this.vehiclesPerSecond = vehiclesPerSecond;
        this.averageVehiclesSpeed = averageVehiclesSpeed;
    }


    public Long getIntersectionId() {
        return intersectionId;
    }

    public void setIntersectionId(Long intersectionId) {
        this.intersectionId = intersectionId;
    }

    public Long getSemaphoreId() {
        return semaphoreId;
    }

    public void setSemaphoreId(Long semaphoreId) {
        this.semaphoreId = semaphoreId;
    }

    public Double getSemaphoreLatitude() {
        return semaphoreLatitude;
    }

    public void setSemaphoreLatitude(Double semaphoreLatitude) {
        this.semaphoreLatitude = semaphoreLatitude;
    }

    public Double getSemaphoreLonditude() {
        return semaphoreLonditude;
    }

    public void setSemaphoreLonditude(Double semaphoreLonditude) {
        this.semaphoreLonditude = semaphoreLonditude;
    }

    public Long getSemaphoreTimestampUTC() {
        return semaphoreTimestampUTC;
    }

    public void setSemaphoreTimestampUTC(Long semaphoreTimestampUTC) {
        this.semaphoreTimestampUTC = semaphoreTimestampUTC;
    }

    public Short getGreenLightDuration() {
        return greenLightDuration;
    }

    public void setGreenLightDuration(Short greenLightDuration) {
        this.greenLightDuration = greenLightDuration;
    }

    public Byte getGreenLightStatus() {
        return greenLightStatus;
    }

    public void setGreenLightStatus(Byte greenLightStatus) {
        this.greenLightStatus = greenLightStatus;
    }

    public Byte getYellowLightStatus() {
        return yellowLightStatus;
    }

    public void setYellowLightStatus(Byte yellowLightStatus) {
        this.yellowLightStatus = yellowLightStatus;
    }

    public Byte getRedLightStatus() {
        return redLightStatus;
    }

    public void setRedLightStatus(Byte redLightStatus) {
        this.redLightStatus = redLightStatus;
    }

    public Short getVehiclesPerSecond() {
        return vehiclesPerSecond;
    }

    public void setVehiclesPerSecond(Short vehiclesPerSecond) {
        this.vehiclesPerSecond = vehiclesPerSecond;
    }

    public Short getAverageVehiclesSpeed() {
        return averageVehiclesSpeed;
    }

    public void setAverageVehiclesSpeed(Short averageVehiclesSpeed) {
        this.averageVehiclesSpeed = averageVehiclesSpeed;
    }

}
