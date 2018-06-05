package it.uniroma2.sdcc.trafficcontrol.entity;


import org.apache.storm.tuple.Tuple;

import java.io.Serializable;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class SemaphoreSensor implements Serializable {

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

    public static SemaphoreSensor getSemaphoreSensorFrom(Tuple tuple) throws ClassCastException, IllegalArgumentException {
        Long semaphoreId = tuple.getLongByField(SEMAPHORE_ID);
        Double semaphoreLatitude = tuple.getDoubleByField(SEMAPHORE_LATITUDE);
        Double semaphoreLongitude = tuple.getDoubleByField(SEMAPHORE_LONGITUDE);
        Long semaphoreTimestampUTC = tuple.getLongByField(SEMAPHORE_TIMESTAMP_UTC);
        Short averageVehiclesSpeed = tuple.getShortByField(AVERAGE_VEHICLES_SPEED);
        Short vehiclesNumber = tuple.getShortByField(VEHICLES);

        return new SemaphoreSensor(
                semaphoreId,
                semaphoreLatitude,
                semaphoreLongitude,
                semaphoreTimestampUTC,
                averageVehiclesSpeed,
                vehiclesNumber
        );
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
        result = 31 * result + semaphoreId.hashCode();
        return result;
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf
                .append(String.format(
                        "SEMAPHORE ID: %d\ttimestamp (UTC) - %d\n",
                        semaphoreId,
                        semaphoreTimestampUTC
                ))
                .append(String.format("Latitude: %1$,.2f\n", semaphoreLatitude))
                .append(String.format("Longitude: %1$,.2f\n", semaphoreLongitude))
                .append(String.format("Mean vehicles speed: %d", averageVehiclesSpeed));

        return buf.toString();
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
