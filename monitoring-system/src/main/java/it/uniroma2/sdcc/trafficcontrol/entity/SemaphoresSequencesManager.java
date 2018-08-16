package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.Params.Properties.SEMAPHORES_SEQUENCES_FILE;
import static it.uniroma2.sdcc.trafficcontrol.entity.SemaphoresSequence.SequenceType.LONGITUDINAL;

@Getter
@Setter
@ToString
public class SemaphoresSequencesManager implements Serializable {

    private List<SemaphoresSequence> semaphoresSequences;
    private Double roadDelta;

    public SemaphoresSequencesManager() {
        semaphoresSequences = new ArrayList<>();
    }

    public SemaphoresSequencesManager(List<SemaphoresSequence> semaphoresSequences, Double roadDelta) {
        this.semaphoresSequences = semaphoresSequences;
        this.roadDelta = roadDelta;
    }

    public void dispatchMobileSensorToSequence() {
        // TODO dispatcha in funzione dell'errore road_delta
    }

    public static SemaphoresSequencesManager getInstanceFrom(String JSONStructurePath, Double roadDelta) {
        try {
            // Read json file data to String
            byte[] jsonData = Files.readAllBytes(Paths.get(SEMAPHORES_SEQUENCES_FILE));
            // Convert json string to object
            SemaphoresSequencesManager semaphoresSequencesManager = new ObjectMapper().readValue(jsonData, SemaphoresSequencesManager.class);
            if (roadDelta != null) {
                semaphoresSequencesManager.setRoadDelta(roadDelta);
            }
            return semaphoresSequencesManager;
        } catch (IOException e) {
            System.err.println(String.format("Impossibile creare un'istanza dal path: %s", JSONStructurePath));
            e.printStackTrace();
            return null;
        }
    }

    public static List<SemaphoresSequence> getsemaphoresSequenceFromBoltsList(SequencesBolts sequencesBolts) {
        List<SemaphoresSequence> semaphoresSequences = new ArrayList<>(sequencesBolts.getSequenceBoltList().size());
        sequencesBolts.getSequenceBoltList().forEach(sb -> semaphoresSequences.add(sb.getSemaphoresSequence()));
        return semaphoresSequences;
    }

    public static SemaphoresSequence findSequenceFrom(RichMobileSensor richMobileSensor) {

        // TODO

        return new SemaphoresSequence(
                new ArrayList<Long>() {{add(1L);add(2L);add(3L);add(4L);}},
                32.3244,
                12.4324,
                LONGITUDINAL,
                null,
                null
        );
    }

    public static SemaphoresSequence findSequenceFrom(Double position) {
        return new SemaphoresSequence();
    }

    @SuppressWarnings("Duplicates")
    public void sortList() {
        if (semaphoresSequences == null) throw new IllegalStateException("La lista è vuota");

        semaphoresSequences.sort((o1, o2) -> {
            double delta = o1.getCongestionGrade() - o2.getCongestionGrade();
            if (delta > 1) return -1;
            else if (delta == 0) return 0;
            else return 1;
        });
    }

}
