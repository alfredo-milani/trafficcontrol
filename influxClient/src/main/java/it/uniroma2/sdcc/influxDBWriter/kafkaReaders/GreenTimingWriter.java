package it.uniroma2.sdcc.influxDBWriter.kafkaReaders;

import com.fasterxml.jackson.databind.JsonNode;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class GreenTimingWriter extends AbstractKafkaWriter {

    private final String tableName;

    public GreenTimingWriter(String dbName, String topicName, String tableName) throws IOException {
        super(dbName, topicName);
        this.tableName = tableName;
    }

    public GreenTimingWriter(String dbName, String topicName, Long poolTimeout, String tableName) throws IOException {
        super(dbName, topicName, poolTimeout);
        this.tableName = tableName;
    }

    @Override
    protected BatchPoints attachPointTo(BatchPoints batchPoints, JsonNode jsonNode) {
        String semaphoreSide = jsonNode
                .get(SEMAPHORE_SIDE).toString()
                .replace("\"", "");
        switch (semaphoreSide) {
            case SEMAPHORE_SIDE_EVEN:
                semaphoreSide = "even";
                break;

            case SEMAPHORE_SIDE_ODD:
                semaphoreSide = "odd";
                break;

            default:
                semaphoreSide = "unknown";
                break;
        }

        batchPoints.point(
                Point.measurement(tableName)
                        .time(getAtomicTimestamp(), TimeUnit.MILLISECONDS)
                        .addField(INTERSECTION_ID, jsonNode.get(INTERSECTION_ID).longValue())
                        .addField(SEMAPHORE_SIDE, semaphoreSide)
                        .addField(GREEN_TEMPORIZATION_VALUE, jsonNode.get(GREEN_TEMPORIZATION_VALUE).shortValue())
                        .build()
        );

        return batchPoints;
    }

}
