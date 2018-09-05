package it.uniroma2.sdcc.trafficcontrol.entity.greenTiming;

import it.uniroma2.sdcc.trafficcontrol.entity.BaseIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.SemaphoreSensor;

import java.util.ArrayList;
import java.util.List;

public class GreenTemporizationIntersection extends BaseIntersection {

    List<SemaphoreSensor> semaphoreSensorsEven;
    List<SemaphoreSensor> semaphoreSensorsOdd;


    public GreenTemporizationIntersection(Long intersectionId) {

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
