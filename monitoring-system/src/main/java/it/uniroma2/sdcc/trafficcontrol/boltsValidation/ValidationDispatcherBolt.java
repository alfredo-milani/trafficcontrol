package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.RichMobileSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.MOBILE_SENSOR_STREAM;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.SEMAPHORE_SENSOR_STREAM;

public class ValidationDispatcherBolt extends AbstractDispatcherBolt {

    @Override
    protected void declareStreamValue(Tuple tuple, Map<String, Values> streamValueHashMap) throws BadTuple {
        RichSensor richSensor;
        if ((richSensor = RichSemaphoreSensor.getInstanceFrom(tuple)) != null) {
            streamValueHashMap.put(SEMAPHORE_SENSOR_STREAM, new Values(((RichSemaphoreSensor) richSensor).getSemaphoreId(), richSensor));
        } else if ((richSensor = RichMobileSensor.getInstanceFrom(tuple)) != null) {
            streamValueHashMap.putIfAbsent(MOBILE_SENSOR_STREAM, new Values(((RichMobileSensor) richSensor).getMobileId(), richSensor));
        } else {
            throw new BadTuple();
        }
    }

    @Override
    protected void declareStreamField(Map<String, Fields> streamFieldMap) {
        streamFieldMap.put(
                SEMAPHORE_SENSOR_STREAM,
                new Fields(SEMAPHORE_ID, SEMAPHORE_SENSOR)
        );

        streamFieldMap.put(
                MOBILE_SENSOR_STREAM,
                new Fields(MOBILE_ID, MOBILE_SENSOR)
        );
    }

}
