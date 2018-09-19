package it.uniroma2.sdcc.trafficcontrol.entity.thirdQuery;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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

    public void dispatchMobileSensorToSequence(RichMobileSensor richMobileSensor) {
        SemaphoresSequence semaphoresSequence = findSequenceFrom(richMobileSensor);
        addSensorTo(semaphoresSequence, richMobileSensor);
    }

    public static SemaphoresSequencesManager getInstanceFrom(Config config) {
        try {
            // Read json file data to String
            @Cleanup InputStream semaphoresSequencesInputStream = config.getSemaphoresSequencesInputStream();
            @Cleanup BufferedInputStream semaphoresSequencesBufferedInputStream = new BufferedInputStream(semaphoresSequencesInputStream);
            // Convert json string to object
            SemaphoresSequencesManager semaphoresSequencesManager = new ObjectMapper().readValue(
                    semaphoresSequencesBufferedInputStream,
                    SemaphoresSequencesManager.class
            );

            if (config.getRoadDelta() != null) {
                semaphoresSequencesManager.setRoadDelta(config.getRoadDelta());
            }
            return semaphoresSequencesManager;
        } catch (IOException e) {
            System.err.println(String.format(
                    "Impossibile creare un'istanza dal path: %s",
                    config.getSemaphoresSequencesFilename()
            ));
            e.printStackTrace();
            return null;
        }
    }

    public static List<SemaphoresSequence> getsemaphoresSequenceFromBoltsList(SequencesBolts sequencesBolts) {
        List<SemaphoresSequence> semaphoresSequences = new ArrayList<>(sequencesBolts.getSequenceBoltList().size());
        sequencesBolts.getSequenceBoltList().forEach(sb -> semaphoresSequences.add(sb.getSemaphoresSequence()));
        return semaphoresSequences;
    }

    public SemaphoresSequence findSequenceFrom(RichMobileSensor richMobileSensor) {
        for (SemaphoresSequence s : semaphoresSequences) {
            switch (s.getSequenceType()) {
                case LONGITUDINAL:
                    if (richMobileSensor.getMobileLongitude() >= s.getMainCoordinate() - roadDelta &&
                            richMobileSensor.getMobileLongitude() <= s.getMainCoordinate() + roadDelta) {
                        return s;
                    }
                    break;

                case LATITUDINAL:
                    if (richMobileSensor.getMobileLatitude() >= s.getMainCoordinate() - roadDelta &&
                            richMobileSensor.getMobileLatitude() <= s.getMainCoordinate() + roadDelta) {
                        return s;
                    }
                    break;
            }
        }
        return null;
    }

    public static SemaphoresSequence findSequenceFrom(Double position) {
        return new SemaphoresSequence();
    }

    public SemaphoresSequence getFirstSequence() {
        try {
            return semaphoresSequences.get(0);
        } catch (IndexOutOfBoundsException e) {
            return  null;
        }
    }

    public boolean removeSemaphoreSequence(SemaphoresSequence semaphoresSequence) {
        return semaphoresSequences.remove(semaphoresSequence);
    }

    public boolean addSemaphoreSequence(SemaphoresSequence semaphoresSequence) {
        if (semaphoresSequences.contains(semaphoresSequence)) return false;
        return semaphoresSequences.add(semaphoresSequence);
    }

    public boolean updateSemaphoresSequenceWith(SemaphoresSequence semaphoresSequence) {
        int index = semaphoresSequences.indexOf(semaphoresSequence);
        if (index == -1) return false;
        semaphoresSequences.get(index).setSensorsInSequence(semaphoresSequence.getSensorsInSequence());
        semaphoresSequences.get(index).setCongestionGrade(semaphoresSequence.getCongestionGrade());
        return true;
    }

    public boolean addSensorTo(SemaphoresSequence semaphoresSequence, RichMobileSensor mobileSensor) {
        int index = semaphoresSequences.indexOf(semaphoresSequence);
        if (index == -1) return false;
        semaphoresSequences.get(index).getSensorsInSequence().add(mobileSensor);
        return true;
    }

    @SuppressWarnings("Duplicates")
    public void sortListByCongestionGrade() {
        if (semaphoresSequences == null) throw new IllegalStateException("La lista non è stata inizializzata");
        if (semaphoresSequences.size() == 0) throw new ArrayIndexOutOfBoundsException("Lista vuota");

        semaphoresSequences.sort((o1, o2) -> {
            double delta = o1.getCongestionGrade() - o2.getCongestionGrade();
            if (delta > 1) return -1;
            else if (delta == 0) return 0;
            else return 1;
        });
    }

}
