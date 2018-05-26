package it.uniroma2.sdcc.trafficcontrol.utils;


import java.io.Serializable;
import java.util.Objects;

public class IntersectionItem implements Serializable {
    private Long intersectionId;
    private Long semaphoreId;
    private Double semaphoreLatitude;
    private Double semaphoreLongitude;
    private Short averageVehiclesSpeed;
    private Short numberVehicle;

    //per la prima query
    public IntersectionItem(Long intersectionId, Long semaphoreId, Double semaphoreLatitude, Double semaphoreLongitude, Short averageVehiclesSpeed) {
        this.intersectionId = intersectionId;
        this.semaphoreId = semaphoreId;
        this.semaphoreLatitude = semaphoreLatitude;
        this.semaphoreLongitude = semaphoreLongitude;
        this.averageVehiclesSpeed = averageVehiclesSpeed;
    }


    //per la seconda query

    public IntersectionItem(Long intersectionId, Long semaphoreId, Short numberVehicle) {
        this.intersectionId = intersectionId;
        this.semaphoreId = semaphoreId;
        this.numberVehicle = numberVehicle;
    }

    public Short getNumberVehicle() {
        return numberVehicle;
    }

    public void setNumberVehicle(Short numberVehicle) {
        this.numberVehicle = numberVehicle;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null || !(obj instanceof IntersectionItem))
            return false;

        IntersectionItem other = (IntersectionItem) obj;


        return Objects.equals(this.intersectionId, other.intersectionId);
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

    public Short getAverageVehiclesSpeed() {
        return averageVehiclesSpeed;
    }

    public void setAverageVehiclesSpeed(Short averageVehiclesSpeed) {
        this.averageVehiclesSpeed = averageVehiclesSpeed;
    }



}
