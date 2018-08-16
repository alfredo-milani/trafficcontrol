package it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoresSequence;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoresSequencesManager;
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

        // - lat/long tipo (manhattan): 40.752107, -74.004805 -> 281 11th ave Fino a 40.741725, -73.978209 kips bay
        // Con una distanza in line retta di 2,8 km. Con una larghezza di strada di long -73.994320 - -73.994259 = 0.000061

        SemaphoresSequence semaphoresSequenceMobileSensor = SemaphoresSequencesManager.findSequenceFrom(richMobileSensor);

        for (SequencesBolts.SequenceBolt sb : sequencesBolts.getSequenceBoltList()) {
            if (sb.getSemaphoresSequence().equals(semaphoresSequenceMobileSensor)) {
                return new HashMap<String, Values>() {{
                    put(sb.getStreamName(), new Values(richMobileSensor));
                }};
            }
        }

        throw new BadTuple("Errore nel processamento della tupla");
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
