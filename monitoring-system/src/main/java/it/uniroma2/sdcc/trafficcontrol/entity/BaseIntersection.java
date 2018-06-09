package it.uniroma2.sdcc.trafficcontrol.entity;

import org.apache.storm.tuple.Tuple;

import java.util.LinkedList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;

public class BaseIntersection implements BasicSensor {

    private final static Long NOT_INITIALIZED = null;

    protected final Long intersectionId;
    protected List<SemaphoreSensor> semaphoreSensors;
    private Long oldestSemaphoreTimestam;

    public BaseIntersection(Long intersectionId) {
        this.intersectionId = intersectionId;
        this.semaphoreSensors = new LinkedList<>();
        this.oldestSemaphoreTimestam = NOT_INITIALIZED;
    }

    public static Long getIntersectionIdFrom(Tuple tuple) throws ClassCastException, IllegalArgumentException {
        return tuple.getLongByField(INTERSECTION_ID);
    }

    public Long getIntersectionId() {
        return intersectionId;
    }

    // TODO CONTROLLARE TIMESTAMP PRIMA DELL AGGIUNTA PER VEDERE SE è SCADUTA
    public boolean addSemaphoreSensor(SemaphoreSensor semaphoreSensor) {
        if (oldestSemaphoreTimestam == NOT_INITIALIZED) {
            oldestSemaphoreTimestam = semaphoreSensor.getSemaphoreTimestampUTC();
        }

        return semaphoreSensors.add(semaphoreSensor);
    }

    public List<SemaphoreSensor> getSemaphoreSensors() {
        return semaphoreSensors;
    }

    public void setSemaphoreSensors(List<SemaphoreSensor> semaphoreSensors) {
        this.semaphoreSensors = semaphoreSensors;
    }

}
