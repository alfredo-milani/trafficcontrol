package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.RESTfulAPI.RESTfulAPI;
import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractAuthenticationBolt;
import it.uniroma2.sdcc.trafficcontrol.constants.RESTfulServices;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;

public class MobileAuthByEndpointBolt extends AbstractAuthenticationBolt {

    public MobileAuthByEndpointBolt(String cacheName) {
        super(cacheName);
    }

    @Override
    public void execute(Tuple tuple) {
        RichMobileSensor mobileSensor = (RichMobileSensor) tuple.getValueByField(MOBILE_SENSOR);

        boolean mobileInSystem;
        synchronized (cacheManager.getCacheManager()) {
            // Double checked lock
            if (!(mobileInSystem = cacheManager.isKeyInCache(mobileSensor.getMobileId()))) {
                if (mobileInSystem = RESTfulAPI.sensorExistsWithIdFromEndpoint(mobileSensor.getMobileId(), RESTfulServices.GET_MOBILE_SENSOR_ID)) {
                    cacheManager.put(mobileSensor.getMobileId(), mobileSensor.getMobileId());
                }
            }
        }

        if (mobileInSystem) {
            collector.emit(new Values(mobileSensor));
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(MOBILE_SENSOR));
    }

}