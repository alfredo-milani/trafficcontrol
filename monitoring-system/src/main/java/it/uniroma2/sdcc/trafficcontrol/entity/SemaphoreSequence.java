package it.uniroma2.sdcc.trafficcontrol.entity;

import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;

import java.util.List;

public class SemaphoreSequence {

    private final List<RichMobileSensor> sensorsInSequence;
    private final int numberOfSemaphore;
    private final Double initialCoordinate;
    private final Double finalCoordinate;
    private final SequenceType sequenceType;

    private enum SequenceType {
        LATITUDINAL,
        LONGITUDINAL
    }

    public SemaphoreSequence(List<RichMobileSensor> sensorsInSequence,
                             int numberOfSemaphore, Double initialCoordinate,
                             Double finalCoordinate, SequenceType sequenceType) {
        this.sensorsInSequence = sensorsInSequence;
        this.numberOfSemaphore = numberOfSemaphore;
        this.initialCoordinate = initialCoordinate;
        this.finalCoordinate = finalCoordinate;
        this.sequenceType = sequenceType;
    }

}
