package it.uniroma2.sdcc.sensorSimulatorAsSpecifications;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sdcc.trafficcontrol.entity.thirdQuery.SemaphoresSequence;
import it.uniroma2.sdcc.trafficcontrol.entity.thirdQuery.SemaphoresSequencesManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

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
    private final long waitTimeMillis;
    private final long sensorsNum;
    private final SemaphoresSequencesManager semaphoresSequencesManager;
    private final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/YYYY HH:mm:ss");
    // Per calcolare un timestap condiviso tra più threads
    private final static AtomicLong LAST_TIME_MS = new AtomicLong();

    public MobileSensorProducer(KafkaProducer<String, String> producer, String topicName,
                                long waitTimeMillis, long sensorsNum,
                                SemaphoresSequencesManager semaphoresSequencesManager) {
        this.producer = producer;
        this.topicName = topicName;
        this.waitTimeMillis = waitTimeMillis;
        this.sensorsNum = sensorsNum;
        this.semaphoresSequencesManager = semaphoresSequencesManager;
    }

    @SuppressWarnings({"InfiniteLoopStatement", "Duplicates"})
    @Override
    public void run() {
        List<SemaphoresSequence> semaphoresSequences = semaphoresSequencesManager.getSemaphoresSequences();
        int lenght = semaphoresSequences.size();
        while (true) {
            System.out.println(String.format("TIME: %s", dateTimeFormatter.format(LocalDateTime.now())));

            for (long i = 1; i <= sensorsNum; ++i) {
                SemaphoresSequence semaphoresSequence = semaphoresSequences.get(ThreadLocalRandom.current().nextInt(0, lenght));

                mobileId = i;
                mobileTimestampUTC = getAtomicTimestamp();
                mobileSpeed = (short) ThreadLocalRandom.current().nextInt(0, 150 + 1);
                switch (semaphoresSequence.getSequenceType()) {
                    case LATITUDINAL:
                        mobileLatitude = semaphoresSequence.getMainCoordinate();
                        mobileLonditude = ThreadLocalRandom.current().nextDouble(
                                semaphoresSequence.getInitialCoordinate(),
                                semaphoresSequence.getFinalCoordinate() + 1
                        );
                        break;

                    case LONGITUDINAL:
                        mobileLonditude = semaphoresSequence.getMainCoordinate();
                        mobileLatitude = ThreadLocalRandom.current().nextDouble(
                                semaphoresSequence.getInitialCoordinate(),
                                semaphoresSequence.getFinalCoordinate() + 1
                        );
                        break;
                }

                try {
                    String jsonString = mapper.writeValueAsString(this);
                    producer.send(new ProducerRecord<>(topicName, jsonString));
                    System.out.println(String.format("\t> Tupla inviata\t\t| %s |", jsonString));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
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
