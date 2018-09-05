package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.RESTfulAPI.RESTfulAPI;
import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractAuthenticationBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadEndpointException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;

public class MobileAuthByEndpointBolt extends AbstractAuthenticationBolt {

    // File di configurazione
    private final static Config config;
    static {
        config = Config.getInstance();
        try {
            // Caricamento proprietà
            config.loadIfHasNotAlreadyBeenLoaded();
        } catch (IOException e) {
            System.err.println(String.format(
                    "%s: error while reading configuration file",
                    MobileAuthByEndpointBolt.class.getSimpleName()
            ));
            e.printStackTrace();
        }
    }
    private final String mobileSensorEndpoint;

    public MobileAuthByEndpointBolt(String cacheName) {
        super(cacheName);

        mobileSensorEndpoint = config.getMobileSensorsEndpoint();
        if (mobileSensorEndpoint == null) {
            throw new BadEndpointException(String.format(
                    "Si deve specificare un endpoint valido. Endpoint corrente: \"%s\"",
                    mobileSensorEndpoint
            ));
        }
    }

    @Override
    public void execute(Tuple tuple) {
        RichMobileSensor mobileSensor = (RichMobileSensor) tuple.getValueByField(MOBILE_SENSOR);

        boolean mobileInSystem;
        synchronized (cacheManager.getCacheManager()) {
            // Double checked lock
            if (!(mobileInSystem = cacheManager.isKeyInCache(mobileSensor.getMobileId()))) {
                if (mobileInSystem = RESTfulAPI.sensorExistsWithIdFromEndpoint(mobileSensor.getMobileId(), mobileSensorEndpoint)) {
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