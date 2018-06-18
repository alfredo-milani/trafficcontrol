package it.uniroma2.sdcc.trafficcontrol.entity;

import it.uniroma2.sdcc.trafficcontrol.exceptions.BadIntersectionTopology;
import it.uniroma2.sdcc.trafficcontrol.exceptions.MeanIntersectoinSpeedNotReady;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_NUMBER_TO_COMPUTE_MEAN_SPEED;

public class MeanSpeedIntersection extends BaseIntersection {

    private int meanIntersectionSpeed;

    public MeanSpeedIntersection(Long intersectionId) {
        super(intersectionId);
    }

    public MeanSpeedIntersection(Long intersectionId, SemaphoreSensor semaphoreSensor) {
        super(intersectionId);
        if (semaphoreSensor == null) {
            throw new IllegalArgumentException("semaphoreSensor can not be null");
        }
        addSemaphoreSensor(semaphoreSensor);
    }

    private int meanOf(Short... values) {
        int sum = 0;
        for (int v : values) {
            sum += v;
        }
        return sum / values.length;
    }

    private Short[] getShortArrayOfSemaphores() {
        int size = semaphoreSensors.size();
        Short[] meanSpeed = new Short[size];
        for (int i = 0; i < size; ++i) {
            meanSpeed[i] = semaphoreSensors.get(i).getAverageVehiclesSpeed();
        }
        return meanSpeed;
    }

    public void computeMeanIntersectionSpeed() throws BadIntersectionTopology {
        int semaphoreListSize = semaphoreSensors.size();
        if (semaphoreListSize < SEMAPHORE_NUMBER_TO_COMPUTE_MEAN_SPEED) {
            throw new MeanIntersectoinSpeedNotReady(String.format(
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

        meanIntersectionSpeed = meanOf(getShortArrayOfSemaphores());
    }

    public int getMeanIntersectionSpeed() {
        return meanIntersectionSpeed;
    }

}
