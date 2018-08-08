package it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.RichMobileSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;

public class DirectionDispatcherBolt extends AbstractDispatcherBolt {

    @Override
    protected Map<String, Values> declareStreamValue(Tuple tuple) throws BadTuple {
        RichMobileSensor richMobileSensor = RichMobileSensor.getInstanceFrom(tuple);
        if (richMobileSensor == null) {
            throw new BadTuple();
        }

        // TODO
        // - leggi da file (o da file proprietà) la sequenza dei semafori (del tipo: "seq1":{ "lat":12,2, "long":44,5}, ecc.)
        // - crea una mappa dai valori letti dal file
        // -

        return new HashMap<String, Values>() {{
            // put(DEFAULT_STREAM, new Values(RichMobileSensor., richMobileSensor));
        }};
    }

    @Override
    protected Map<String, Fields> declareStreamField() {
        return new HashMap<String, Fields>() {{
            put(DEFAULT_STREAM, new Fields(INTERSECTION_ID, SEMAPHORE_SENSOR));
        }};
    }

}
