package it.uniroma2.sdcc.trafficcontrol.entity.secondQuery;

import com.tdunning.math.stats.TDigest;
import it.uniroma2.sdcc.trafficcontrol.entity.BaseIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.SemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadIntersectionTopology;
import it.uniroma2.sdcc.trafficcontrol.exceptions.MedianIntersectionNotReady;
import lombok.Getter;

import static it.uniroma2.sdcc.trafficcontrol.entity.secondQuery.MedianIntersectionManager.COMPRESSION;
import static it.uniroma2.sdcc.trafficcontrol.entity.secondQuery.MedianIntersectionManager.QUANTILE;


public class MedianIntersection extends BaseIntersection {

    @Getter
    private double medianIntersection;

    public MedianIntersection(Long intersectionId) {
        super(intersectionId);
    }

    public MedianIntersection(Long intersectionId, SemaphoreSensor semaphoreSensor) {
        super(intersectionId);
        if (semaphoreSensor == null) {
            throw new IllegalArgumentException("semaphoreSensor can not be null");
        }
        addSemaphoreSensor(semaphoreSensor);
    }

    private Double computeMedian() {
        TDigest tDigestIntesection = TDigest.createAvlTreeDigest(COMPRESSION);
        for (SemaphoreSensor semaphoreSensor : semaphoreSensors) {
            tDigestIntesection.add(semaphoreSensor.getVehiclesNumber());
        }
        return tDigestIntesection.quantile(QUANTILE);
    }

    public void computeMedianVehiclesIntersection(int semaphoreNumber) throws BadIntersectionTopology {
        int semaphoreListSize = semaphoreSensors.size();
        if (semaphoreListSize < semaphoreNumber) {
            throw new MedianIntersectionNotReady(String.format(
                    "Current semaphore list size: %d in intersection id %d",
                    semaphoreListSize,
                    intersectionId
            ));
        } else if (semaphoreListSize > semaphoreNumber) {
            throw new BadIntersectionTopology(String.format(
                    "Semaphore list size: %d in intersection id %d. This software only manage intersection speed of 4 semaphores at time",
                    semaphoreListSize,
                    intersectionId
            ));
        }

        medianIntersection = computeMedian();
    }

    @Override
    public String toString() {
        return " medianIntersection=" + medianIntersection;
    }

}
