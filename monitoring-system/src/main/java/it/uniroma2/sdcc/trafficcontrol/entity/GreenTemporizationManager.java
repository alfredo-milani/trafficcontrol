package it.uniroma2.sdcc.trafficcontrol.entity;

import java.util.ArrayList;
import java.util.List;

public class GreenTemporizationManager extends BaseIntersectionManager {

    List<SemaphoreSensor> semaphoreSensorsEven;
    List<SemaphoreSensor> semaphoreSensorsOdd;


    public GreenTemporizationManager(Long intersectionId) {

        super(intersectionId);
        semaphoreSensorsEven = new ArrayList<>();
        semaphoreSensorsOdd =  new ArrayList<>();
    }

    @Override
    public boolean addSemaphoreSensor(SemaphoreSensor semaphoreSensor) {

        if (semaphoreSensor.getSemaphoreId() % 2 == 0)
            return semaphoreSensorsEven.add(semaphoreSensor);
        else  return semaphoreSensorsOdd.add(semaphoreSensor);

    }

    public List<SemaphoreSensor> getSemaphoreSensorsEven() {
        return semaphoreSensorsEven;
    }

    public List<SemaphoreSensor> getSemaphoreSensorsOdd() {
        return semaphoreSensorsOdd;
    }
}
