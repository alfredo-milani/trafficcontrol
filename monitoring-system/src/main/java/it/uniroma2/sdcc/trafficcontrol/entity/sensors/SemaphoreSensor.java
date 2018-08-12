package it.uniroma2.sdcc.trafficcontrol.entity.sensors;


import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.text.SimpleDateFormat;
import java.util.Date;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

@Getter
@Setter
@EqualsAndHashCode
public class SemaphoreSensor implements ITupleObject, ISensor {

    private Long semaphoreId;
    private Double semaphoreLatitude;
    private Double semaphoreLongitude;
    private Long semaphoreTimestampUTC;
    @EqualsAndHashCode.Exclude
    private Short averageVehiclesSpeed;
    @EqualsAndHashCode.Exclude
    private Short vehiclesNumber;

    public SemaphoreSensor(Long semaphoreId, Double semaphoreLatitude,
                           Double semaphoreLongitude, Long semaphoreTimestampUTC,
                           Short averageVehiclesSpeed, Short vehiclesNumber) {
        this.semaphoreId = semaphoreId;
        this.semaphoreLatitude = semaphoreLatitude;
        this.semaphoreLongitude = semaphoreLongitude;
        this.semaphoreTimestampUTC = semaphoreTimestampUTC;
        this.averageVehiclesSpeed = averageVehiclesSpeed;
        this.vehiclesNumber = vehiclesNumber;
    }

    public static SemaphoreSensor getInstanceFrom(RichSemaphoreSensor richSemaphoreSensor) {
        return new SemaphoreSensor(
                richSemaphoreSensor.getSemaphoreId(),
                richSemaphoreSensor.getSemaphoreLatitude(),
                richSemaphoreSensor.getSemaphoreLongitude(),
                richSemaphoreSensor.getSemaphoreTimestampUTC(),
                richSemaphoreSensor.getAverageVehiclesSpeed(),
                richSemaphoreSensor.getVehiclesNumber()
        );
    }

    @Override
    public String getJsonStringFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put(SEMAPHORE_ID, semaphoreId);
        objectNode.put(SEMAPHORE_LATITUDE, semaphoreLatitude);
        objectNode.put(SEMAPHORE_LONGITUDE, semaphoreLongitude);
        objectNode.put(SEMAPHORE_TIMESTAMP_UTC, semaphoreTimestampUTC);
        objectNode.put(AVERAGE_VEHICLES_SPEED, averageVehiclesSpeed);
        objectNode.put(VEHICLES, vehiclesNumber);

        return objectNode.toString();
    }

    @Override
    public String toString() {
        return String.format(
                "Semaphore %d - <timestamp - %s>\n",
                semaphoreId,
                new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(semaphoreTimestampUTC))
        ) +
                String.format("Latitude: %1$,.2f\n", semaphoreLatitude) +
                String.format("Longitude: %1$,.2f\n", semaphoreLongitude) +
                String.format("Mean vehicles speed: %d", averageVehiclesSpeed);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        super.clone();

        return new SemaphoreSensor(
                semaphoreId,
                semaphoreLatitude,
                semaphoreLongitude,
                semaphoreTimestampUTC,
                averageVehiclesSpeed,
                vehiclesNumber
        );
    }

}
