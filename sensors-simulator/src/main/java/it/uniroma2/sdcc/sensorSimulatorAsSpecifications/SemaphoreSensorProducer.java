package it.uniroma2.sdcc.sensorSimulatorAsSpecifications;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

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

    private final static int SEMAPHORE_PER_INTERSECTION = 4;
    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaProducer<String, String> producer;
    private final String topicName;
    private final long waitTimeMillis;
    private final long sensorsNum;
    private final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/YYYY HH:mm:ss");
    // Per calcolare un timestap condiviso tra più threads
    private final static AtomicLong LAST_TIME_MS = new AtomicLong();

    public SemaphoreSensorProducer(KafkaProducer<String, String> producer, String topicName,
                                   long waitTimeMillis, long sensorsNum) {
        this.producer = producer;
        this.topicName = topicName;
        this.waitTimeMillis = waitTimeMillis;
        this.sensorsNum = sensorsNum;
    }

    @SuppressWarnings({"InfiniteLoopStatement", "Duplicates"})
    @Override
    public void run() {
        while (true) {
            System.out.println(String.format("TIME: %s", dateTimeFormatter.format(LocalDateTime.now())));

            long intersectionsNumber = sensorsNum / SEMAPHORE_PER_INTERSECTION;
            for (long i = 1; i <= intersectionsNumber; ++i) { // Per ogni intersezione
                for (long j = 1; j <= SEMAPHORE_PER_INTERSECTION; ++j) { // Per ogni semaforo contenuto in un'intersezione
                    intersectionId = i;
                    semaphoreId = j;
                    semaphoreLatitude = ThreadLocalRandom.current().nextDouble(0, 90 + 1);
                    semaphoreLonditude = ThreadLocalRandom.current().nextDouble(0, 180 + 1);
                    semaphoreTimestampUTC = getAtomicTimestamp();
                    greenLightDuration = (short) ThreadLocalRandom.current().nextInt(0, 300 + 1);
                    greenLightStatus = (byte) ThreadLocalRandom.current().nextInt(0, 127 + 1);
                    yellowLightStatus = (byte) ThreadLocalRandom.current().nextInt(0, 127 + 1);
                    redLightStatus = (byte) ThreadLocalRandom.current().nextInt(0, 127 + 1);
                    vehiclesPerSecond = (short) ThreadLocalRandom.current().nextInt(0, 100 + 1);
                    averageVehiclesSpeed = (short) ThreadLocalRandom.current().nextInt(0, 150 + 1);

                    try {
                        String jsonString = mapper.writeValueAsString(this);
                        producer.send(new ProducerRecord<>(topicName, jsonString));
                        System.out.println(String.format("\t> Tupla inviata\t\t| %s |", jsonString));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
            }

            System.out.println("\n");

            try {
                Thread.sleep(waitTimeMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Long getAtomicTimestamp() {
        Long lastTime, timestampToUse = System.currentTimeMillis();

        do {
            lastTime = LAST_TIME_MS.get();
            if (lastTime >= timestampToUse) timestampToUse = lastTime + 1;
        } while (!LAST_TIME_MS.compareAndSet(lastTime, timestampToUse));

        return timestampToUse;
    }

}
