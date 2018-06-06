package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.RESTfulAPI.RESTfulAPI;
import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractAuthenticationBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.RichMobileSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;

public class MobileAuthDBBolt extends AbstractAuthenticationBolt {

    public MobileAuthDBBolt(String cacheName) {
        super(cacheName);
    }

    @Override
    public void execute(Tuple tuple) {
        RichMobileSensor mobileSensor = (RichMobileSensor) tuple.getValueByField(MOBILE_SENSOR);

        boolean mobileInSystem;
        synchronized (cacheManager.getCacheManager()) {
            // Double checked lock
            if (!(mobileInSystem = cacheManager.isKeyInCache(mobileSensor.getMobileId()))) {
                if (mobileInSystem = RESTfulAPI.mobileSensorExist(mobileSensor.getMobileId())) {
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