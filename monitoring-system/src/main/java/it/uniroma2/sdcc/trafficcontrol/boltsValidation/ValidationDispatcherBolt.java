package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.RichMobileSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.MOBILE_SENSOR_STREAM;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.SEMAPHORE_SENSOR_STREAM;

public class ValidationDispatcherBolt extends AbstractDispatcherBolt {

    @Override
    protected void doBefore() {

    }

    @Override
    protected String computeValuesToEmit(Tuple tuple) throws BadTuple {
        RichSemaphoreSensor semaphoreSensor;
        RichMobileSensor mobileSensor;

        if ((semaphoreSensor = RichSemaphoreSensor.getInstanceFrom(tuple)) != null) {
            streamValueHashMap.put(SEMAPHORE_SENSOR_STREAM, new Values(semaphoreSensor.getSemaphoreId(), semaphoreSensor));
            return SEMAPHORE_SENSOR_STREAM;
        } else if ((mobileSensor = RichMobileSensor.getInstanceFrom(tuple)) != null) {
            streamValueHashMap.putIfAbsent(MOBILE_SENSOR_STREAM, new Values(mobileSensor.getMobileId(), mobileSensor));
            return MOBILE_SENSOR_STREAM;
        } else {
            throw new BadTuple();
        }
    }

    @Override
    protected void doAfter() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                SEMAPHORE_SENSOR_STREAM,
                new Fields(SEMAPHORE_ID, SEMAPHORE_SENSOR)
        );

        declarer.declareStream(
                MOBILE_SENSOR_STREAM,
                new Fields(MOBILE_ID, MOBILE_SENSOR)
        );
    }

}
