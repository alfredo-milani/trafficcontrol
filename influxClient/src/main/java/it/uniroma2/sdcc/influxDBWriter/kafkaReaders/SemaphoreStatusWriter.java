package it.uniroma2.sdcc.influxDBWriter.kafkaReaders;

import com.fasterxml.jackson.databind.JsonNode;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class SemaphoreStatusWriter extends AbstractKafkaWriter {

    private final String tableName;

    public SemaphoreStatusWriter(String dbName, String topicName, String tableName) throws IOException {
        super(dbName, topicName);
        this.tableName = tableName;
    }

    public SemaphoreStatusWriter(String dbName, String topicName, Long poolTimeout, String tableName) throws IOException {
        super(dbName, topicName, poolTimeout);
        this.tableName = tableName;
    }

    @Override
    protected BatchPoints computeBatchPoints(JsonNode jsonNode, String dbName) {
        // Creation and definition of a batch containing points for influxDB
        BatchPoints batchPoints = BatchPoints
                .database(dbName)
                .retentionPolicy(RETANTION_POLICY)
                .consistency(InfluxDB.ConsistencyLevel.QUORUM)
                .build();

        batchPoints.point(
                Point.measurement(tableName)
                        .time(getAtomicTimestamp(), TimeUnit.MILLISECONDS)
                        .addField(INTERSECTION_ID, jsonNode.get(INTERSECTION_ID).longValue())
                        .addField(SEMAPHORE_ID, jsonNode.get(SEMAPHORE_ID).longValue())
                        .addField(GREEN_LIGHT_STATUS, jsonNode.get(GREEN_LIGHT_STATUS).toString().replace("\"", ""))
                        .addField(YELLOW_LIGHT_STATUS, jsonNode.get(YELLOW_LIGHT_STATUS).toString().replace("\"", ""))
                        .addField(RED_LIGHT_STATUS, jsonNode.get(RED_LIGHT_STATUS).toString().replace("\"", ""))
                        .build()
        );

        return batchPoints;
    }

}
