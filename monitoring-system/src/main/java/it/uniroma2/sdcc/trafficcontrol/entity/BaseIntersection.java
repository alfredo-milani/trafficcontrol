package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.tuple.Tuple;

import java.util.LinkedList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;

public class BaseIntersection implements ITupleObject, ISensor {

    @Getter
    protected final Long intersectionId;
    protected List<SemaphoreSensor> semaphoreSensors;
    @Getter
    protected Long oldestSemaphoreTimestamp;

    public BaseIntersection(Long intersectionId) {
        this.intersectionId = intersectionId;
        this.semaphoreSensors = new LinkedList<>();
    }

    public static Long getIntersectionIdFrom(Tuple tuple) throws ClassCastException, IllegalArgumentException {
        return tuple.getLongByField(INTERSECTION_ID);
    }

    public boolean addSemaphoreSensor(SemaphoreSensor semaphoreSensor) {
        List<SemaphoreSensor> semaphoreSensorsCopy = getSemaphoreSensors();
        if (semaphoreSensorsCopy.size() > 0) {
            // Controllo se esiste già un sensore con lo stesso id
            for (SemaphoreSensor s : semaphoreSensorsCopy) {
                if (s.getSemaphoreId().equals(semaphoreSensor.getSemaphoreId()) &&
                        s.getSemaphoreTimestampUTC() < semaphoreSensor.getSemaphoreTimestampUTC()) {
                    // Se esiste un sensore con stesso id e timestamp maggiore del precedente allora
                    // probabilmente a breve mi saranno inviate anche le nuove tuple degli altri sensori
                    // dei semafori dell'intersezione quindi rimuovo le vecchie informazioni
                    semaphoreSensors.removeAll(semaphoreSensorsCopy);
                    return addSemaphoreSensor(semaphoreSensor);
                }
            }

            if (semaphoreSensor.getSemaphoreTimestampUTC() < oldestSemaphoreTimestamp) {
                oldestSemaphoreTimestamp = semaphoreSensor.getSemaphoreTimestampUTC();
            }
        } else {
            oldestSemaphoreTimestamp = semaphoreSensor.getSemaphoreTimestampUTC();
        }

        return semaphoreSensors.add(semaphoreSensor);
    }

    @Override
    public String getJsonStringFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put(INTERSECTION_ID, intersectionId);

        return objectNode.toString();
    }

    public List<SemaphoreSensor> getSemaphoreSensors() {
        List<SemaphoreSensor> semaphoreSensors = Lists.newLinkedList();
        semaphoreSensors.addAll(this.semaphoreSensors);
        return ImmutableList.copyOf(semaphoreSensors);
    }

}
