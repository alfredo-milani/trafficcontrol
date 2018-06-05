package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.KAFKA_RAW_TUPLE;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;

public class BaseIntersectionManager {

    private final static ObjectMapper mapper = new ObjectMapper();

    protected final Long intersectionId;
    protected List<SemaphoreSensor> semaphoreSensors;

    public BaseIntersectionManager(Long intersectionId) {
        this.intersectionId = intersectionId;
    }

    public static Long getIntersectionIdFrom(Tuple tuple) throws IOException {
        String rawTuple = tuple.getStringByField(KAFKA_RAW_TUPLE);
        JsonNode jsonNode = mapper.readTree(rawTuple);

        return jsonNode.get(INTERSECTION_ID).asLong();
    }

    public Long getIntersectionId() {
        return intersectionId;
    }

    public boolean addSemaphoreSensor(SemaphoreSensor semaphoreSensor) {
        return semaphoreSensors.add(semaphoreSensor);
    }

    public List<SemaphoreSensor> getSemaphoreSensors() {
        return semaphoreSensors;
    }

    public void setSemaphoreSensors(List<SemaphoreSensor> semaphoreSensors) {
        this.semaphoreSensors = semaphoreSensors;
    }

}
