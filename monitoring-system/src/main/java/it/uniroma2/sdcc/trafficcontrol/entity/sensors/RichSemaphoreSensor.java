package it.uniroma2.sdcc.trafficcontrol.entity.sensors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.Setter;
import org.apache.storm.tuple.Tuple;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.KAFKA_RAW_TUPLE;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

@Getter
@Setter
public class RichSemaphoreSensor implements ITupleObject, ISensor {

    private Long intersectionId;
    private Long semaphoreId;
    private Double semaphoreLatitude;
    private Double semaphoreLongitude;
    private Long semaphoreTimestampUTC;
    private Short greenLightDuration;
    private Byte greenLightStatus;
    private Byte yellowLightStatus;
    private Byte redLightStatus;
    private Short vehiclesNumber;
    private Short averageVehiclesSpeed;

    public RichSemaphoreSensor(Long intersectionId, Long semaphoreId,
                               Double semaphoreLatitude, Double semaphoreLongitude,
                               Long semaphoreTimestampUTC, Short greenLightDuration,
                               Byte greenLightStatus, Byte yellowLightStatus,
                               Byte redLightStatus, Short vehiclesNumber,
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
        this.vehiclesNumber = vehiclesNumber;
        this.averageVehiclesSpeed = averageVehiclesSpeed;
    }

    public static RichSemaphoreSensor getInstanceFrom(Tuple tuple) {
        try {
            String rawTuple = tuple.getStringByField(KAFKA_RAW_TUPLE);
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
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String getJsonStringFromInstance() {
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
        objectNode.put(VEHICLES, vehiclesNumber);
        objectNode.put(AVERAGE_VEHICLES_SPEED, averageVehiclesSpeed);

        return objectNode.toString();
    }

}
