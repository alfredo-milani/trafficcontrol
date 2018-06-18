package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.storm.tuple.Tuple;

import java.util.LinkedList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;

public class BaseIntersection implements ITupleObject, ISensor {

    private final static Long NOT_INITIALIZED = null;

    protected final Long intersectionId;
    protected List<SemaphoreSensor> semaphoreSensors;
    private Long oldestSemaphoreTimestamp;

    public BaseIntersection(Long intersectionId) {
        this.intersectionId = intersectionId;
        this.semaphoreSensors = new LinkedList<>();
        this.oldestSemaphoreTimestamp = NOT_INITIALIZED;
    }

    public static Long getIntersectionIdFrom(Tuple tuple) throws ClassCastException, IllegalArgumentException {
        return tuple.getLongByField(INTERSECTION_ID);
    }

    public Long getIntersectionId() {
        return intersectionId;
    }

    public boolean addSemaphoreSensor(SemaphoreSensor semaphoreSensor) {
        if (oldestSemaphoreTimestamp == NOT_INITIALIZED) {
            oldestSemaphoreTimestamp = semaphoreSensor.getSemaphoreTimestampUTC();
        }

        return semaphoreSensors.add(semaphoreSensor);
    }

    public List<SemaphoreSensor> getSemaphoreSensors() {
        return semaphoreSensors;
    }

    public void setSemaphoreSensors(List<SemaphoreSensor> semaphoreSensors) {
        this.semaphoreSensors = semaphoreSensors;
    }

    @Override
    public String getJsonStringFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put(INTERSECTION_ID, intersectionId);

        return objectNode.toString();
    }

    public Long getOldestSemaphoreTimestamp() {
        return oldestSemaphoreTimestamp;
    }

    public void setOldestSemaphoreTimestamp(Long oldestSemaphoreTimestamp) {
        this.oldestSemaphoreTimestamp = oldestSemaphoreTimestamp;
    }

}
