package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple;

public class MeanSpeedIntersectionManager extends BaseIntersectinoManager {

    private final static ObjectMapper mapper = new ObjectMapper();

    private final static int MEAN_SPEED_COMPUTED = -1;

    private int meanIntersectionSpeed;

    public MeanSpeedIntersectionManager(Long intersectionId) {
        super(intersectionId);
    }

    private void computeIntersectionMeanSpeed(Short... values) {
        int count = values.length, sum = 0;
        for (int v : values) {
            sum += v;
        }
        meanIntersectionSpeed = sum / count;
    }

    public boolean addSemaphoreSensor(SemaphoreSensor semaphoreSensor) {
        boolean response = super.addSemaphoreSensor(semaphoreSensor);

        // TODO verificare numero semafori e fare media
        if (semaphoreSensors.size() == SemaphoreSensorTuple.SEMAPHORE_NUMBER_TO_COMPUTE_SPEED_MEAN) {

            computeIntersectionMeanSpeed();
        }

        return response;
    }

    public int getMeanIntersectionSpeed() {
        return meanIntersectionSpeed;
    }

}
