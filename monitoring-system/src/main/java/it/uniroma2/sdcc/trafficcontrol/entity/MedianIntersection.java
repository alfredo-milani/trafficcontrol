package it.uniroma2.sdcc.trafficcontrol.entity;

import com.tdunning.math.stats.TDigest;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadIntersectionTopology;
import it.uniroma2.sdcc.trafficcontrol.exceptions.MeanIntersectoinSpeedNotReady;
import it.uniroma2.sdcc.trafficcontrol.exceptions.MedianIntersectionNotReady;
import it.uniroma2.sdcc.trafficcontrol.utils.IntersectionItem;

import java.util.ArrayList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_NUMBER_TO_COMPUTE_MEAN_SPEED;

public class MedianIntersection extends BaseIntersection{
    private double compression = 100;
    private double quantile = 0.5; //mediana

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



    private Double medianCalulator() {
        TDigest tDigestIntesection = TDigest.createAvlTreeDigest(compression);
        int size = semaphoreSensors.size();
        for (int i = 0; i < size; ++i) {
             tDigestIntesection.add(semaphoreSensors.get(i).getVehiclesNumber());
        }
        return tDigestIntesection.quantile(quantile);
    }

    public void computeMedianVehiclesIntersection() throws BadIntersectionTopology {
        int semaphoreListSize = semaphoreSensors.size();
        if (semaphoreListSize < SEMAPHORE_NUMBER_TO_COMPUTE_MEAN_SPEED) {
            throw new MedianIntersectionNotReady(String.format(
                    "Current semaphore list size: %d in intersection id %d",
                    semaphoreListSize,
                    intersectionId
            ));
        } else if (semaphoreListSize > SEMAPHORE_NUMBER_TO_COMPUTE_MEAN_SPEED) {
            throw new BadIntersectionTopology(String.format(
                    "Semaphore list size: %d in intersection id %d. This software only manage intersection speed of 4 semaphores at time",
                    semaphoreListSize,
                    intersectionId
            ));
        }

        medianIntersection = medianCalulator();
    }

    @Override
    public String toString() {
        return " medianIntersection=" + medianIntersection ;
    }
}
