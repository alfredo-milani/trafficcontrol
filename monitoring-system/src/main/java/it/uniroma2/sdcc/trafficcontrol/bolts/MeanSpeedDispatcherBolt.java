package it.uniroma2.sdcc.trafficcontrol.bolts;

import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;

public class MeanSpeedDispatcherBolt extends AbstractDispatcherBolt {

    @Override
    protected void doBefore() {

    }

    @Override
    protected String computeValuesToEmit(Tuple tuple) throws BadTuple {
        RichSemaphoreSensor richSemaphoreSensor = RichSemaphoreSensor.getInstanceFrom(tuple);
        if (richSemaphoreSensor == null) {
            throw new BadTuple(String.format("Bad tuple: %s", tuple.toString()));
        }

        streamValueHashMap.put(
                DEFAULT_STREAM,
                new Values(richSemaphoreSensor.getSemaphoreId(), richSemaphoreSensor)
        );

        return DEFAULT_STREAM;
    }

    @Override
    protected void doAfter() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(INTERSECTION_ID, SEMAPHORE_SENSOR));
    }

}
