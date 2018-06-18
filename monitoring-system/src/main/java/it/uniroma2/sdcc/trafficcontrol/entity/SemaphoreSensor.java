package it.uniroma2.sdcc.trafficcontrol.entity;


import com.fasterxml.jackson.databind.node.ObjectNode;

import java.text.SimpleDateFormat;
import java.util.Date;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class SemaphoreSensor implements ITupleObject, ISensor {

    private Long semaphoreId;
    private Double semaphoreLatitude;
    private Double semaphoreLongitude;
    private Long semaphoreTimestampUTC;
    private Short averageVehiclesSpeed;
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

    public Short getAverageVehiclesSpeed() {
        return averageVehiclesSpeed;
    }

    public void setAverageVehiclesSpeed(Short averageVehiclesSpeed) {
        this.averageVehiclesSpeed = averageVehiclesSpeed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SemaphoreSensor)) {
            return false;
        }

        SemaphoreSensor other = (SemaphoreSensor) o;
        return semaphoreId.equals(other.semaphoreId) &&
                semaphoreTimestampUTC.equals(other.semaphoreTimestampUTC);
    }

    @Override
    public int hashCode() {
        int result = 17;
        int countHash = (averageVehiclesSpeed ^ (averageVehiclesSpeed >>> 32));
        result = 31 * result + countHash;
        result = (int) (31 * result + semaphoreId.hashCode() + semaphoreTimestampUTC);
        return result;
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

    public Short getVehiclesNumber() {
        return vehiclesNumber;
    }

}
