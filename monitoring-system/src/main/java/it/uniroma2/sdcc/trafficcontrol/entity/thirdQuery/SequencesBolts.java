package it.uniroma2.sdcc.trafficcontrol.entity.thirdQuery;

import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.CONGESTION_COMPUTATION_BASE_BOLT;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.CONGESTION_COMPUTATION_BASE_STREAM;

@Getter
@Setter
public class SequencesBolts implements Serializable {

    private final List<SequenceBolt> sequenceBoltList;
    private final SemaphoresSequencesManager semaphoresSequencesManager;

    @Getter
    public class SequenceBolt implements Serializable {

        private final String boltName;
        private final String streamName;
        private final SemaphoresSequence semaphoresSequence;

        public SequenceBolt(String boltName, String streamName, SemaphoresSequence semaphoresSequence) {
            this.boltName = boltName;
            this.streamName = streamName;
            this.semaphoresSequence = semaphoresSequence;
        }

    }

    public SequencesBolts(List<SequenceBolt> sequenceBoltList) {
        this.sequenceBoltList = sequenceBoltList;
        semaphoresSequencesManager = new SemaphoresSequencesManager();
    }

    public SequencesBolts(AppConfig appConfig) {
        semaphoresSequencesManager = SemaphoresSequencesManager.getInstanceFrom(appConfig);
        if (semaphoresSequencesManager == null) {
            throw new IllegalArgumentException(String.format(
                    "Impossibile creare l'istanza dal file: %s",
                    appConfig.getSemaphoresSequencesFilename()
            ));
        }

        sequenceBoltList = new ArrayList<>(semaphoresSequencesManager.getSemaphoresSequences().size());
        for (int i = 0; i < semaphoresSequencesManager.getSemaphoresSequences().size(); ++i) {
            sequenceBoltList.add(i, new SequenceBolt(
                    String.format("%s_%d", CONGESTION_COMPUTATION_BASE_BOLT, i),
                    String.format("%s_%d", CONGESTION_COMPUTATION_BASE_STREAM, i),
                    semaphoresSequencesManager.getSemaphoresSequences().get(i)
            ));
        }
    }

}