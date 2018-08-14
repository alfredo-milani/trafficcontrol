package it.uniroma2.sdcc.trafficcontrol.entity;

import java.util.List;

public class SemaphoresSequencesManager {

    private final List<SemaphoreSequence> semaphoreSequences;
    private final Double ROAD_DELTA;

    public SemaphoresSequencesManager(List<SemaphoreSequence> semaphoreSequences,
                                      Double ROAD_DELTA) {
        this.semaphoreSequences = semaphoreSequences;
        this.ROAD_DELTA = ROAD_DELTA;
        // TODO leggere il file JSON e creare la lista delle sequenze
    }

    public void dispatchMobileSensorToSequence() {
        // TODO dispatcha in funzoine dell'errore road_delta
    }

}
