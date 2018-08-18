package it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoresSequence;
import it.uniroma2.sdcc.trafficcontrol.entity.SequencesBolts;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;

public class SequencesDispatcherBolt extends AbstractDispatcherBolt {

    private SequencesBolts sequencesBolts;

    public SequencesDispatcherBolt(SequencesBolts sequencesBolts) {
        this.sequencesBolts = sequencesBolts;
    }

    public SequencesDispatcherBolt(String JSONStructure, Double roadDelta) {
        sequencesBolts = new SequencesBolts(JSONStructure, roadDelta);
    }

    @Override
    protected Map<String, Values> declareStreamValue(Tuple tuple) throws BadTuple {
        RichMobileSensor richMobileSensor = RichMobileSensor.getInstanceFrom(tuple);
        if (richMobileSensor == null) {
            throw new BadTuple();
        }

        SemaphoresSequence semaphoresSequenceMobileSensor =
                sequencesBolts.getSemaphoresSequencesManager().findSequenceFrom(richMobileSensor);
        for (SequencesBolts.SequenceBolt sb : sequencesBolts.getSequenceBoltList()) {
            if (sb.getSemaphoresSequence().equals(semaphoresSequenceMobileSensor)) {
                return new HashMap<String, Values>() {{
                    put(sb.getStreamName(), new Values(richMobileSensor));
                }};
            }
        }

        return new HashMap<>();
    }

    @Override
    protected Map<String, Fields> declareStreamField() {
        Map<String, Fields> stringFieldsMap = new HashMap<>(sequencesBolts.getSequenceBoltList().size());
        sequencesBolts.getSequenceBoltList().forEach(
                sb -> stringFieldsMap.put(sb.getStreamName(), new Fields(MOBILE_SENSOR))
        );
        return stringFieldsMap;
    }

}
