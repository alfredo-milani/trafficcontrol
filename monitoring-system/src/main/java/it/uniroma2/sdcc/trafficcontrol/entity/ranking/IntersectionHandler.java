package it.uniroma2.sdcc.trafficcontrol.entity.ranking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.KAFKA_RAW_TUPLE;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;

public class IntersectionHandler {

    private final static ObjectMapper mapper = new ObjectMapper();

    private final static int MEAN_SPEED_COMPUTED = -1;

    private final Long intersectionId;
    private List<SemaphoreSensor> semaphoreSensors;
    private int meanIntersectionSpeed;

    public IntersectionHandler(Long intersectionId) {
        this.intersectionId = intersectionId;
    }

    private void computeIntersectionMeanSpeed(Short... values) {
        int count = values.length, sum = 0;
        for (int v : values) {
            sum += v;
        }
        meanIntersectionSpeed = sum / count;
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
        boolean response = semaphoreSensors.add(semaphoreSensor);
        // TODO verificare numero semafori e fare media
        if (/* esistono almeno 2 semapfori */ true) {

            computeIntersectionMeanSpeed();
        }

        return response;
    }

    public List<SemaphoreSensor> getSemaphoreSensors() {
        return semaphoreSensors;
    }

    public void setSemaphoreSensors(List<SemaphoreSensor> semaphoreSensors) {
        this.semaphoreSensors = semaphoreSensors;
        // TODO verificare numero semafori e fare media
    }

    public int getMeanIntersectionSpeed() {
        return meanIntersectionSpeed;
    }

    public void setMeanIntersectionSpeed(int meanIntersectionSpeed) {
        this.meanIntersectionSpeed = meanIntersectionSpeed;
    }

}
