package it.uniroma2.sdcc.influxDBWriter.kafkaReaders;

import com.fasterxml.jackson.databind.JsonNode;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.CongestedSequenceJsonFileds.*;

public class CongestedIntersectionWriter extends AbstractKafkaWriter {

    private final String tableName;

    public CongestedIntersectionWriter(String dbName, String topicName, Config config, Long poolTimeout, String tableName) {
        super(dbName, topicName, config, poolTimeout);
        this.tableName = tableName;
    }

    @Override
    protected BatchPoints attachPointTo(BatchPoints batchPoints, JsonNode jsonNode) {
        JsonNode intersectionNode = jsonNode
                .get(CONGESTED_SEQUENCE_INSTANCE);
        String sequenceString = intersectionNode
                .get(SEMAPHORES_SEQUENCE).toString()
                .replace("[", "")
                .replace("]", "")
                .replace(",", ", ");

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
