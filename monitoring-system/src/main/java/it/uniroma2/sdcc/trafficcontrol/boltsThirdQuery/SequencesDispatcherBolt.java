package it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;

public class SequencesDispatcherBolt extends AbstractDispatcherBolt {

    @Override
    protected Map<String, Values> declareStreamValue(Tuple tuple) throws BadTuple {
        RichMobileSensor richMobileSensor = RichMobileSensor.getInstanceFrom(tuple);
        if (richMobileSensor == null) {
            throw new BadTuple();
        }

        // - leggi da file (o da file proprietà) la sequenza dei semafori (del tipo: "seq1":{ "lat":12,2, "long":44,5}, ecc.)
        // - crea una mappa dai valori letti dal file
        // - lat/long tipo (manhattan): 40.752107, -74.004805 -> 281 11th ave Fino a 40.741725, -73.978209 kips bay
        // Con una distanza in line retta di 2,8 km. Con una larghezza di strada di long -73.994320 - -73.994259 = 0.000061

        return new HashMap<String, Values>() {{
            // put(DEFAULT_STREAM, new Values(RichMobileSensor., richMobileSensor));
        }};
    }

    @Override
    protected Map<String, Fields> declareStreamField() {
        return new HashMap<String, Fields>() {{
            put(DEFAULT_STREAM, new Fields(INTERSECTION_ID, MOBILE_SENSOR));
        }};
    }

}
