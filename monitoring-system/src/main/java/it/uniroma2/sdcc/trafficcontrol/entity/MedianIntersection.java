package it.uniroma2.sdcc.trafficcontrol.entity;

import com.tdunning.math.stats.TDigest;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadIntersectionTopology;
import it.uniroma2.sdcc.trafficcontrol.exceptions.MedianIntersectionNotReady;
import org.apache.storm.tuple.Tuple;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.INTERSECTION_MEDIAN_VEHICLES_OBJECT;


public class MedianIntersection extends BaseIntersection{
    private double compression = 100;
    private double quantile = 0.5; //mediana

    private double medianIntersectionCalculated;

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

        medianIntersectionCalculated = medianCalulator();
    }

    public static MedianIntersection getIstanceFrom(Tuple tuple) {
        return (MedianIntersection) tuple.getValueByField(INTERSECTION_MEDIAN_VEHICLES_OBJECT);
    }

    @Override
    public String toString() {
        return " medianIntersectionCalculated=" + medianIntersectionCalculated;
    }

    public double getMedianIntersectionCalculated() {
        return medianIntersectionCalculated;
    }
}
