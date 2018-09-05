package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.RESTfulAPI.RESTfulAPI;
import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractAuthenticationBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadEndpointException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config.SEMAPHORES_SENSORS_ENDPOINT;

public class SemaphoreAuthByEndpointBolt extends AbstractAuthenticationBolt {

    // File di configurazione onfigurazione
    private final static Config config;
    static {
        config = Config.getInstance();
        try {
            // Caricamento proprietà
            config.loadIfHasNotAlreadyBeenLoaded();
        } catch (IOException e) {
            System.err.println(String.format(
                    "%s: error while reading configuration file",
                    SemaphoreAuthByEndpointBolt.class.getSimpleName()
            ));
            e.printStackTrace();
        }
    }
    private final String semaphoreSensorEndpoint;

    public SemaphoreAuthByEndpointBolt(String cacheName) {
        super(cacheName);

        semaphoreSensorEndpoint = (String) config.get(SEMAPHORES_SENSORS_ENDPOINT);
        if (semaphoreSensorEndpoint == null) {
            throw new BadEndpointException(String.format(
                    "Si deve specificare un endpoint valido. Endpoint corrente: \"%s\"",
                    semaphoreSensorEndpoint
            ));
        }
    }

    @Override
    public void execute(Tuple tuple) {
        RichSemaphoreSensor semaphoreSensor = (RichSemaphoreSensor) tuple.getValueByField(SEMAPHORE_SENSOR);

        boolean semaphoreInSystem;
        synchronized (cacheManager.getCacheManager()) {
            // Double checked lock
            if (!(semaphoreInSystem = cacheManager.isKeyInCache(semaphoreSensor.getSemaphoreId()))) {
                if (semaphoreInSystem = RESTfulAPI.sensorExistsWithIdFromEndpoint(semaphoreSensor.getSemaphoreId(), semaphoreSensorEndpoint)) {
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
