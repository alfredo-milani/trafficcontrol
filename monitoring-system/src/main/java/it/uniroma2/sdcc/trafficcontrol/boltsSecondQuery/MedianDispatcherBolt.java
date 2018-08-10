package it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.GLOBAL_MEDIAN_CALCULATOR_STREAM;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.MEDIAN_CALCULATOR_STREAM;

public class MedianDispatcherBolt extends AbstractDispatcherBolt {

    @Override
    protected Map<String, Values> declareStreamValue(Tuple tuple) throws BadTuple {
        RichSemaphoreSensor richSemaphoreSensor = RichSemaphoreSensor.getInstanceFrom(tuple);
        if (richSemaphoreSensor == null) {
            throw new BadTuple();
        }

        return new HashMap<String, Values>() {{
            put(MEDIAN_CALCULATOR_STREAM , new Values(richSemaphoreSensor.getSemaphoreId(), richSemaphoreSensor));
            put(GLOBAL_MEDIAN_CALCULATOR_STREAM , new Values(richSemaphoreSensor.getSemaphoreId(), richSemaphoreSensor));

        }};
    }

    @Override
    protected Map<String, Fields> declareStreamField() {
        return new HashMap<String, Fields>() {{
            put(MEDIAN_CALCULATOR_STREAM, new Fields(INTERSECTION_ID, SEMAPHORE_SENSOR));
            put(GLOBAL_MEDIAN_CALCULATOR_STREAM, new Fields(SEMAPHORE_SENSOR));
        }};
    }
    
    
    
}
