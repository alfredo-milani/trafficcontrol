package it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.IWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoresSequence;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoresSequencesManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.SEMAPHORE_SEQUENCE_OBJECT;

public class SequenceSelectorWindowedBolt extends AbstractWindowedBolt {

    private final SemaphoresSequencesManager semaphoresSequencesManager;

    public SequenceSelectorWindowedBolt(int windowSizeInSeconds, int emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        semaphoresSequencesManager = new SemaphoresSequencesManager();
    }

    public SequenceSelectorWindowedBolt(int windowSizeInSeconds, int emitFrequencyInSeconds,
                                        List<SemaphoresSequence> semaphoresSequences, Double roadDelta) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        semaphoresSequencesManager = new SemaphoresSequencesManager(semaphoresSequences, roadDelta);
    }

    public SequenceSelectorWindowedBolt(int windowSizeInSeconds, int emitFrequencyInSeconds,
                                        String JSONFileStructure, Double roadDelta) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        semaphoresSequencesManager = SemaphoresSequencesManager.getInstanceFrom(JSONFileStructure, roadDelta);
    }

    @Override
    protected void onTick(OutputCollector collector, IWindow<Tuple> eventsWindow) {
        SemaphoresSequence oldSemaphoresSequence = semaphoresSequencesManager.getFirstSequence();
        Double oldCongestionGrade = 0D;
        if (oldSemaphoresSequence != null) oldCongestionGrade = oldSemaphoresSequence.getCongestionGrade();

        eventsWindow.getExpiredEvents().forEach(t -> {
            SemaphoresSequence semaphoresSequence = (SemaphoresSequence) t.getValueByField(SEMAPHORE_SEQUENCE_OBJECT);
            semaphoresSequence.setCongestionGrade(0D);
            semaphoresSequencesManager.updateSemaphoresSequenceWith(semaphoresSequence);
        });

        eventsWindow.getNewEvents().forEach(t -> {
            SemaphoresSequence semaphoresSequence = (SemaphoresSequence) t.getValueByField(SEMAPHORE_SEQUENCE_OBJECT);
            semaphoresSequencesManager.updateSemaphoresSequenceWith(semaphoresSequence);
        });

        try {
            semaphoresSequencesManager.sortListByCongestionGrade();
            // Aggiorniamo la sequenza anche se cambia solo il suo grado di congestione
            if (!semaphoresSequencesManager.getFirstSequence().getCongestionGrade().equals(oldCongestionGrade)) {
                collector.emit(new Values(semaphoresSequencesManager.getFirstSequence()));
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            // Lista vuota
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SEMAPHORE_SEQUENCE_OBJECT));
    }

}
