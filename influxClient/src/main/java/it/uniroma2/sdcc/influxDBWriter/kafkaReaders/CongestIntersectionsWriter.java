package it.uniroma2.sdcc.influxDBWriter.kafkaReaders;

import com.fasterxml.jackson.databind.JsonNode;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.MedianVehiclesIntersectionJsonFields.*;

public class CongestIntersectionsWriter extends AbstractKafkaWriter {

    private final String tableName;

    public CongestIntersectionsWriter(String dbName, String topicName, String tableName) throws IOException {
        super(dbName, topicName);
        this.tableName = tableName;
    }

    public CongestIntersectionsWriter(String dbName, String topicName, Long poolTimeout, String tableName) throws IOException {
        super(dbName, topicName, poolTimeout);
        this.tableName = tableName;
    }

    @Override
    protected BatchPoints attachPointTo(BatchPoints batchPoints, JsonNode jsonNode) {
        JsonNode intersectionsNode = jsonNode.get(HIGHER_MEDIAN);

        Iterator<JsonNode> intersectionIterator = intersectionsNode.elements();
        while (intersectionIterator.hasNext()) {
            JsonNode intersectionElement = intersectionIterator.next();
            batchPoints.point(
                    Point.measurement(tableName)
                            .time(getAtomicTimestamp(), TimeUnit.MILLISECONDS)
                            .addField(INTERSECTION_ID, intersectionElement.get(INTERSECTION_ID).longValue())
                            .addField(MEDIAN_VEHICLES_INTERSECTION, intersectionElement.get(MEDIAN_VEHICLES_INTERSECTION).doubleValue())
                            .build()
            );
        }

        return batchPoints;
    }

}
