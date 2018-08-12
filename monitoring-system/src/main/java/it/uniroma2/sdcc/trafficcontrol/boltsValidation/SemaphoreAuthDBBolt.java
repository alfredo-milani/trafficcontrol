package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.RESTfulAPI.RESTfulAPI;
import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractAuthenticationBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichSemaphoreSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;

public class SemaphoreAuthDBBolt extends AbstractAuthenticationBolt {

    public SemaphoreAuthDBBolt(String cacheName) {
        super(cacheName);
    }

    @Override
    public void execute(Tuple tuple) {
        RichSemaphoreSensor semaphoreSensor = (RichSemaphoreSensor) tuple.getValueByField(SEMAPHORE_SENSOR);

        boolean semaphoreInSystem;
        synchronized (cacheManager.getCacheManager()) {
            // Double checked lock
            if (!(semaphoreInSystem = cacheManager.isKeyInCache(semaphoreSensor.getSemaphoreId()))) {
                if (semaphoreInSystem = RESTfulAPI.semaphoreSensorExists(semaphoreSensor.getSemaphoreId())) {
                    cacheManager.put(semaphoreSensor.getSemaphoreId(), semaphoreSensor.getSemaphoreId());
                }
            }
        }

        if (semaphoreInSystem) {
            collector.emit(new Values(semaphoreSensor));
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SEMAPHORE_SENSOR));
    }

}
