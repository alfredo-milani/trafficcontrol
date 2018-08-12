package it.uniroma2.sdcc.trafficcontrol.entity;

import it.uniroma2.sdcc.trafficcontrol.entity.sensors.SemaphoreSensor;

import java.util.ArrayList;
import java.util.List;

public class GreenTemporization extends BaseIntersection {

    List<SemaphoreSensor> semaphoreSensorsEven;
    List<SemaphoreSensor> semaphoreSensorsOdd;


    public GreenTemporization(Long intersectionId) {

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
