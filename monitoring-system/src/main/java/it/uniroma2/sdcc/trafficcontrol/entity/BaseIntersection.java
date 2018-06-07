package it.uniroma2.sdcc.trafficcontrol.entity;

import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;

public class BaseIntersection implements Serializable {

    protected final Long intersectionId;
    protected List<SemaphoreSensor> semaphoreSensors;

    public BaseIntersection(Long intersectionId) {
        this.intersectionId = intersectionId;
        this.semaphoreSensors = new LinkedList<>();
    }

    public static Long getIntersectionIdFrom(Tuple tuple) throws ClassCastException, IllegalArgumentException {
        return tuple.getLongByField(INTERSECTION_ID);
    }

    public Long getIntersectionId() {
        return intersectionId;
    }

    // TODO CONTROLLARE TIMESTAMP PRIMA DELL AGGIUNTA PER VEDERE SE è SCADUTA
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
