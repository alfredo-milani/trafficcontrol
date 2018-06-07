package it.uniroma2.sdcc.trafficcontrol.entity;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_NUMBER_TO_COMPUTE_SPEED_MEAN;

public class MeanSpeedIntersection extends BaseIntersection {

    private final static int ERROR_IN_INTERSECTION_TOPOLOGY = -2;
    private final static int MEAN_SPEED_NOT_COMPUTED = -1;

    private int meanIntersectionSpeed = MEAN_SPEED_NOT_COMPUTED;

    public MeanSpeedIntersection(Long intersectionId) {
        super(intersectionId);
    }

    private int computeIntersectionMeanSpeed(Short... values) {
        int count = values.length;
        if (count < SEMAPHORE_NUMBER_TO_COMPUTE_SPEED_MEAN) {
            return meanIntersectionSpeed = MEAN_SPEED_NOT_COMPUTED;
        } else if (count > SEMAPHORE_NUMBER_TO_COMPUTE_SPEED_MEAN) {
            return meanIntersectionSpeed = ERROR_IN_INTERSECTION_TOPOLOGY;
        }

        int sum = 0;
        for (int v : values) {
            sum += v;
        }
        return meanIntersectionSpeed = sum / count;
    }

    public int computeIntersectionMeanSpeed() {
        Short[] meanSpeed = new Short[semaphoreSensors.size()];
        for (int i = 0; i < semaphoreSensors.size(); ++i) {
            meanSpeed[i] = semaphoreSensors.get(i).getAverageVehiclesSpeed();
        }

        return computeIntersectionMeanSpeed(meanSpeed);
    }

    public boolean isMeanComputed() {
        return meanIntersectionSpeed > 0;
    }

    public boolean isListReadyForComputation() {
        return semaphoreSensors.size() == SEMAPHORE_NUMBER_TO_COMPUTE_SPEED_MEAN;
    }

    public int getMeanIntersectionSpeed() {
        return meanIntersectionSpeed;
    }

}
