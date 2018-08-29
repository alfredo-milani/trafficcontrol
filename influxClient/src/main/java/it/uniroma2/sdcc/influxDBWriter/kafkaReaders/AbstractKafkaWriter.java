package it.uniroma2.sdcc.influxDBWriter.kafkaReaders;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sdcc.trafficcontrol.utils.ApplicationsProperties;
import lombok.Getter;
import lombok.extern.java.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;
import static it.uniroma2.sdcc.trafficcontrol.utils.ApplicationsProperties.KAFKA_IP_PORT;

@Log
public abstract class AbstractKafkaWriter implements Runnable {

    // Per calcolare un timestap condiviso tra più threads
    private final static AtomicLong LAST_TIME_MS = new AtomicLong();

    // InfluxDB params
    protected final static String RETANTION_POLICY = "autogen";
    // Connect to InfluxDB
    protected final static String INFLUX_DB_URL = "http://localhost:8086";
    protected final static String INFLUX_DB_USERNAME = "root";
    protected final static String INFLUX_DB_PASSWORD = "root";
    private final static InfluxDB influxDB = InfluxDBFactory.connect(INFLUX_DB_URL, INFLUX_DB_USERNAME, INFLUX_DB_PASSWORD);
    private final String dbName;

    // Kafka group.id
    protected final static String KAFKA_GROUP_ID = "readers";
    private final KafkaConsumer<String, String> consumer;
    @Getter
    protected final String topicName;
    private final static Long DEFAULT_POOL_TIMEOUT = 1000L;
    private final Long POOL_TIMEOUT;

    // JSON utility
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static JsonFactory factory = mapper.getFactory();

    public AbstractKafkaWriter(String dbName, String topicName) throws IOException {
        this(dbName, topicName, DEFAULT_POOL_TIMEOUT);
    }

    public AbstractKafkaWriter(String dbName, String topicName, Long poolTimeout) throws IOException {
        // Load properties
        ApplicationsProperties.getInstance().loadProperties();

        // Creating Database
        this.dbName = dbName;
        influxDB.createDatabase(dbName);

        // Sottoscrizione al topic kafka
        consumer = new KafkaConsumer<>(getComsumerProperties());
        this.topicName = topicName;
        consumer.subscribe(Collections.singletonList(topicName));
        POOL_TIMEOUT = poolTimeout;
    }

    private Properties getComsumerProperties() {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS, KAFKA_IP_PORT);
        properties.put(GROUP_ID, KAFKA_GROUP_ID);
        properties.put(KEY_DESERIALIZER, DESERIALIZER_VALUE);
        properties.put(VALUE_DESERIALIZER, DESERIALIZER_VALUE);
        return properties;
    }

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        // Legge dal topic kafka dall'inizio
        // consumer.poll(POOL_TIMEOUT);
        // consumer.seekToBeginning(Collections.EMPTY_LIST);

        try {
            // Consume data from Kafka
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POOL_TIMEOUT);
                if (!records.isEmpty()) {
                    computeRecords(records);
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer has received instruction to wake up");
        } finally {
            log.info("Consumer closing...");
            closeConsumer();
            log.info("Consumer has closed successfully");
        }
    }

    private void computeRecords(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            try {
                String stringToCompute = record.value();
                JsonParser parser = factory.createParser(stringToCompute);
                JsonNode jsonNode = mapper.readTree(parser);
                // Write on influxDB
                influxDB.write(computeBatchPoints(jsonNode, dbName));
            } catch (IOException e) {
                log.info("Bad JSON tuple");
            }
        }
    }

    private BatchPoints computeBatchPoints(JsonNode jsonNode, String dbName) {
        // Creation and definition of a batch containing points for influxDB
        BatchPoints batchPoints = BatchPoints
                .database(dbName)
                .retentionPolicy(RETANTION_POLICY)
                .consistency(InfluxDB.ConsistencyLevel.QUORUM)
                .build();

        return attachPointTo(batchPoints, jsonNode);
    }

    protected abstract BatchPoints attachPointTo(BatchPoints batchPoints, JsonNode jsonNode);

    protected Long getAtomicTimestamp() {
        Long lastTime, timestampToUse = System.currentTimeMillis();

        do {
            lastTime = LAST_TIME_MS.get();
            if (lastTime >= timestampToUse) timestampToUse = lastTime + 1;
        } while (!LAST_TIME_MS.compareAndSet(lastTime, timestampToUse));

        return timestampToUse;
    }

    public void closeConsumer() {
        consumer.close();
    }

}
