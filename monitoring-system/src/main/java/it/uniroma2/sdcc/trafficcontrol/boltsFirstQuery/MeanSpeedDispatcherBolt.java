package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;

public class MeanSpeedDispatcherBolt extends AbstractDispatcherBolt {

    @Override
    protected void declareStreamValue(Tuple tuple, Map<String, Values> streamValueHashMap) throws BadTuple {
        RichSemaphoreSensor richSemaphoreSensor = RichSemaphoreSensor.getInstanceFrom(tuple);
        if (richSemaphoreSensor == null) {
            throw new BadTuple();
        }

        streamValueHashMap.put(
                DEFAULT_STREAM,
                new Values(richSemaphoreSensor.getSemaphoreId(), richSemaphoreSensor)
        );
    }

    @Override
    protected void declareStreamField(Map<String, Fields> streamFieldMap) {
        streamFieldMap.put(
                DEFAULT_STREAM,
                new Fields(INTERSECTION_ID, SEMAPHORE_SENSOR)
        );
    }

}
