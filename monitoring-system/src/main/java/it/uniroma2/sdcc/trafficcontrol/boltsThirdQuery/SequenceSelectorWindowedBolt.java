package it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.bolts.IWindow;
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

    public SequenceSelectorWindowedBolt() {
        semaphoresSequencesManager = new SemaphoresSequencesManager();
    }

    public SequenceSelectorWindowedBolt(List<SemaphoresSequence> semaphoresSequences, Double roadDelta) {
        semaphoresSequencesManager = new SemaphoresSequencesManager(semaphoresSequences, roadDelta);
    }

    public SequenceSelectorWindowedBolt(String JSONFileStructure, Double roadDelta) {
        semaphoresSequencesManager = SemaphoresSequencesManager.getInstanceFrom(JSONFileStructure, roadDelta);
    }

    @Override
    protected void onTick(OutputCollector collector, IWindow<Tuple> eventsWindow) {
        SemaphoresSequence oldSemaphoresSequence = semaphoresSequencesManager.getSemaphoresSequences().size() != 0
                ? semaphoresSequencesManager.getSemaphoresSequences().get(0)
                : null;

        eventsWindow.getExpiredEvents().forEach(t -> {
            SemaphoresSequence semaphoresSequence = (SemaphoresSequence) t.getValueByField(SEMAPHORE_SEQUENCE_OBJECT);
            semaphoresSequencesManager.getSemaphoresSequences().remove(semaphoresSequence);
        });

        eventsWindow.getNewEvents().forEach(t -> {
            SemaphoresSequence semaphoresSequence = (SemaphoresSequence) t.getValueByField(SEMAPHORE_SEQUENCE_OBJECT);
            semaphoresSequencesManager.getSemaphoresSequences().add(semaphoresSequence);
        });

        if (semaphoresSequencesManager.getSemaphoresSequences().size() != 0) {
            semaphoresSequencesManager.sortList();
            if (!semaphoresSequencesManager.getSemaphoresSequences().get(0).equals(oldSemaphoresSequence)) {
                collector.emit(new Values(semaphoresSequencesManager.getSemaphoresSequences().get(0)));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SEMAPHORE_SEQUENCE_OBJECT));
    }

}
