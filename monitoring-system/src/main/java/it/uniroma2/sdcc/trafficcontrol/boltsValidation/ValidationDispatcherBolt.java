package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.ISensor;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.MOBILE_SENSOR_STREAM;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.SEMAPHORE_SENSOR_STREAM;

public class ValidationDispatcherBolt extends AbstractDispatcherBolt {

    @Override
    protected Map<String, Values> declareStreamValue(Tuple tuple) throws BadTuple {
        ISensor sensor;
        if ((sensor = RichSemaphoreSensor.getInstanceFrom(tuple)) != null) {
            ISensor finalSemaphoreSensor = sensor;
            return new HashMap<String, Values>() {{
                put(SEMAPHORE_SENSOR_STREAM, new Values(((RichSemaphoreSensor) finalSemaphoreSensor).getSemaphoreId(), finalSemaphoreSensor));
            }};
        } else if ((sensor = RichMobileSensor.getInstanceFrom(tuple)) != null) {
            ISensor finalMobileSensor = sensor;
            return new HashMap<String, Values>() {{
                put(MOBILE_SENSOR_STREAM, new Values(((RichMobileSensor) finalMobileSensor).getMobileId(), finalMobileSensor));
            }};
        } else {
            throw new BadTuple();
        }
    }

    @Override
    protected Map<String, Fields> declareStreamField() {
        return new HashMap<String, Fields>() {{
            put(SEMAPHORE_SENSOR_STREAM, new Fields(SEMAPHORE_ID, SEMAPHORE_SENSOR));
            put(MOBILE_SENSOR_STREAM, new Fields(MOBILE_ID, MOBILE_SENSOR));
        }};
    }

}
