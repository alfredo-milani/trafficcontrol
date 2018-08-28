package it.uniroma2.sdcc.influxDBWriter.kafkaReaders;

import com.fasterxml.jackson.databind.JsonNode;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.CongestedSequenceJsonFileds.*;

public class CongestedIntersectionWriter extends AbstractKafkaWriter {

    private final String tableName;

    public CongestedIntersectionWriter(String dbName, String topicName, String tableName) throws IOException {
        super(dbName, topicName);
        this.tableName = tableName;
    }

    public CongestedIntersectionWriter(String dbName, String topicName, Long poolTimeout, String tableName) throws IOException {
        super(dbName, topicName, poolTimeout);
        this.tableName = tableName;
    }

    @Override
    protected BatchPoints attachPointTo(BatchPoints batchPoints, JsonNode jsonNode) {
        JsonNode intersectionNode = jsonNode
                .get(CONGESTED_SEQUENCE_INSTANCE);
        String sequenceString = intersectionNode.get(SEMAPHORES_SEQUENCE).toString();
        sequenceString = sequenceString.replace("[", "");
        sequenceString = sequenceString.replace("]", "");
        sequenceString = sequenceString.replace(",", ", ");

        batchPoints.point(
                Point.measurement(tableName)
                        .time(getAtomicTimestamp(), TimeUnit.MILLISECONDS)
                        .addField(CONGESTION_GRADE, intersectionNode.get(CONGESTION_GRADE).doubleValue())
                        .addField(SEMAPHORES_SEQUENCE, sequenceString)
                        .build()
        );

        return batchPoints;
    }

}
