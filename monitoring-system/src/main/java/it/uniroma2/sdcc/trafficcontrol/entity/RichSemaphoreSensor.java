package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.KAFKA_RAW_TUPLE;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class RichSemaphoreSensor implements RichSensor {

    private final static ObjectMapper mapper = new ObjectMapper();

    private Long intersectionId;
    private Long semaphoreId;
    private Double semaphoreLatitude;
    private Double semaphoreLongitude;
    private Long semaphoreTimestampUTC;
    private Short greenLightDuration;
    private Byte greenLightStatus;
    private Byte yellowLightStatus;
    private Byte redLightStatus;
    private Short vehiclesPerSecond;
    private Short averageVehiclesSpeed;

    public RichSemaphoreSensor(Long intersectionId, Long semaphoreId,
                               Double semaphoreLatitude, Double semaphoreLongitude,
                               Long semaphoreTimestampUTC, Short greenLightDuration,
                               Byte greenLightStatus, Byte yellowLightStatus,
                               Byte redLightStatus, Short vehiclesPerSecond,
                               Short averageVehiclesSpeed) {
        this.intersectionId = intersectionId;
        this.semaphoreId = semaphoreId;
        this.semaphoreLatitude = semaphoreLatitude;
        this.semaphoreLongitude = semaphoreLongitude;
        this.semaphoreTimestampUTC = semaphoreTimestampUTC;
        this.greenLightDuration = greenLightDuration;
        this.greenLightStatus = greenLightStatus;
        this.yellowLightStatus = yellowLightStatus;
        this.redLightStatus = redLightStatus;
        this.vehiclesPerSecond = vehiclesPerSecond;
        this.averageVehiclesSpeed = averageVehiclesSpeed;
    }

    public static RichSemaphoreSensor getInstanceFrom(Tuple tuple) {
        try {
            // TODO vedi se lasciare in questo metodo o nel chiamante
            String rawTuple = tuple.getStringByField(KAFKA_RAW_TUPLE);
            // TODO end

            JsonNode jsonNode = mapper.readTree(rawTuple);

            // Verifica correttezza valori tupla
            Long intersectionId = jsonNode.get(INTERSECTION_ID).asLong();
            Long semaphoreId = jsonNode.get(SEMAPHORE_ID).asLong();
            Double semaphoreLatitude = jsonNode.get(SEMAPHORE_LATITUDE).asDouble();
            Double semaphoreLongitude = jsonNode.get(SEMAPHORE_LONGITUDE).asDouble();
            Long semaphoreTimestampUTC = jsonNode.get(SEMAPHORE_TIMESTAMP_UTC).asLong();
            Short greenLightDuration = jsonNode.get(GREEN_LIGHT_DURATION).shortValue();
            Byte greenLightStatus = (byte) jsonNode.get(GREEN_LIGHT_STATUS).asInt();
            Byte yellowLightStatus = (byte) jsonNode.get(YELLOW_LIGHT_STATUS).asInt();
            Byte redLightStatus = (byte) jsonNode.get(RED_LIGHT_STATUS).asInt();
            Short vehiclesPerSecond = jsonNode.get(VEHICLES).shortValue();
            Short averageVehiclesSpeed = jsonNode.get(AVERAGE_VEHICLES_SPEED).shortValue();

            return new RichSemaphoreSensor(
                    intersectionId,
                    semaphoreId,
                    semaphoreLatitude,
                    semaphoreLongitude,
                    semaphoreTimestampUTC,
                    greenLightDuration,
                    greenLightStatus,
                    yellowLightStatus,
                    redLightStatus,
                    vehiclesPerSecond,
                    averageVehiclesSpeed
            );
        } catch (IOException e) {
            return null;
        }
    }

    public String getJsonFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put(INTERSECTION_ID, intersectionId);
        objectNode.put(SEMAPHORE_ID, semaphoreId);
        objectNode.put(SEMAPHORE_LATITUDE, semaphoreLatitude);
        objectNode.put(SEMAPHORE_LONGITUDE, semaphoreLongitude);
        objectNode.put(SEMAPHORE_TIMESTAMP_UTC, semaphoreTimestampUTC);
        objectNode.put(GREEN_LIGHT_DURATION, greenLightDuration);
        objectNode.put(GREEN_LIGHT_STATUS, greenLightStatus);
        objectNode.put(YELLOW_LIGHT_STATUS, yellowLightStatus);
        objectNode.put(RED_LIGHT_STATUS, redLightStatus);
        objectNode.put(VEHICLES, vehiclesPerSecond);
        objectNode.put(AVERAGE_VEHICLES_SPEED, averageVehiclesSpeed);

        return objectNode.toString();
    }

    public Long getIntersectionId() {
        return intersectionId;
    }

    public void setIntersectionId(Long intersectionId) {
        this.intersectionId = intersectionId;
    }

    public Long getSemaphoreId() {
        return semaphoreId;
    }

    public void setSemaphoreId(Long semaphoreId) {
        this.semaphoreId = semaphoreId;
    }

    public Double getSemaphoreLatitude() {
        return semaphoreLatitude;
    }

    public void setSemaphoreLatitude(Double semaphoreLatitude) {
        this.semaphoreLatitude = semaphoreLatitude;
    }

    public Double getSemaphoreLongitude() {
        return semaphoreLongitude;
    }

    public void setSemaphoreLongitude(Double semaphoreLongitude) {
        this.semaphoreLongitude = semaphoreLongitude;
    }

    public Long getSemaphoreTimestampUTC() {
        return semaphoreTimestampUTC;
    }

    public void setSemaphoreTimestampUTC(Long semaphoreTimestampUTC) {
        this.semaphoreTimestampUTC = semaphoreTimestampUTC;
    }

    public Short getGreenLightDuration() {
        return greenLightDuration;
    }

    public void setGreenLightDuration(Short greenLightDuration) {
        this.greenLightDuration = greenLightDuration;
    }

    public Byte getGreenLightStatus() {
        return greenLightStatus;
    }

    public void setGreenLightStatus(Byte greenLightStatus) {
        this.greenLightStatus = greenLightStatus;
    }

    public Byte getYellowLightStatus() {
        return yellowLightStatus;
    }

    public void setYellowLightStatus(Byte yellowLightStatus) {
        this.yellowLightStatus = yellowLightStatus;
    }

    public Byte getRedLightStatus() {
        return redLightStatus;
    }

    public void setRedLightStatus(Byte redLightStatus) {
        this.redLightStatus = redLightStatus;
    }

    public Short getVehiclesPerSecond() {
        return vehiclesPerSecond;
    }

    public void setVehiclesPerSecond(Short vehiclesPerSecond) {
        this.vehiclesPerSecond = vehiclesPerSecond;
    }

    public Short getAverageVehiclesSpeed() {
        return averageVehiclesSpeed;
    }

    public void setAverageVehiclesSpeed(Short averageVehiclesSpeed) {
        this.averageVehiclesSpeed = averageVehiclesSpeed;
    }

}